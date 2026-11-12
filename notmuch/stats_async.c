/**
 * @file
 * Background worker for Notmuch mailbox statistics
 *
 * @authors
 * Copyright (C) 2026 Rob Johnston <rjohnst@hpe.com>
 *
 * @copyright
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * @page notmuch_stats_async Background worker for Notmuch mailbox statistics
 *
 * Computing Notmuch mailbox statistics (total/unread/flagged counts) runs one
 * or more Xapian queries per mailbox.  On a large database this can take
 * several seconds, and until now it was done inline on the UI thread, freezing
 * NeoMutt while it ran.
 *
 * This module moves that work onto a dedicated background thread so the UI
 * stays responsive.  The design deliberately keeps the thread-shared surface
 * tiny:
 *
 * - The **main thread** snapshots everything the worker needs (query strings,
 *   database path and the relevant config values) into plain-old-data job
 *   structures.  It never shares a `struct Mailbox`, `struct Email`, the config
 *   subset, or a Notmuch database handle with the worker.
 * - The **worker thread** opens its own read-only Notmuch database handle, runs
 *   the count queries, and posts back plain integer results.  It calls no
 *   NeoMutt subsystems that assume single-threaded access (no `cs_subset_*`,
 *   no `mutt_debug`, no buffer pool, no UI).
 * - The main thread later matches the results back to live Mailboxes by pointer
 *   identity (validated against the current mailbox list) and applies the
 *   scalar counts.  It never reallocates the emails array from these results.
 */

#include "config.h"
#include <notmuch.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "private.h"
#include "mutt/lib.h"
#include "config/lib.h"
#include "email/lib.h"
#include "core/lib.h"
#include "lib.h"
#include "tag.h"

/**
 * struct NmStatsConfig - Snapshot of config needed to run count queries
 *
 * Taken on the main thread (where reading the config is safe) and handed to the
 * worker.  All strings are owned by this structure.
 */
struct NmStatsConfig
{
  char *unread_tag;      ///< $nm_unread_tag
  char *flagged_tag;     ///< $nm_flagged_tag
  char **exclude_tags;   ///< NULL-terminated array of $nm_exclude_tags, or NULL
  char *config_file;     ///< Notmuch config file (NULL = auto, "" = none)
  char *config_profile;  ///< $nm_config_profile
  int open_timeout;      ///< $nm_open_timeout
  int db_limit;          ///< $nm_db_limit
};

/**
 * struct NmStatsJob - One mailbox's worth of work for the worker
 */
struct NmStatsJob
{
  void *mbox_id;      ///< Opaque Mailbox pointer, used only for matching
  char *db_filename;  ///< Notmuch database path (may be NULL = auto)
  char *db_query;     ///< Query string
  int limit;          ///< Result limit
};

/**
 * struct NmStatsResult - Counts computed for one mailbox
 */
struct NmStatsResult
{
  void *mbox_id;   ///< Opaque Mailbox pointer, used only for matching
  int msg_count;   ///< Total messages
  int msg_unread;  ///< Unread messages
  int msg_flagged; ///< Flagged messages
};

// --- Worker lifecycle ------------------------------------------------------

static pthread_t Worker;                             ///< Worker thread handle
static pthread_mutex_t Mutex = PTHREAD_MUTEX_INITIALIZER; ///< Guards shared state below
static pthread_cond_t Cond = PTHREAD_COND_INITIALIZER;    ///< Wakes the worker
static pthread_once_t Once = PTHREAD_ONCE_INIT;      ///< One-time worker startup
static bool WorkerOk = false;                        ///< Worker started successfully
static bool WorkerStarted = false;                   ///< pthread_create() succeeded

// --- Shared state (guarded by Mutex) ---------------------------------------

static struct NmStatsJob *Jobs = NULL; ///< Pending job batch (input)
static int NJobs = 0;                  ///< Number of pending jobs
static struct NmStatsConfig Cfg = { 0 };///< Config snapshot for the pending batch
static bool HasJob = false;            ///< A batch is waiting to be processed
static bool Busy = false;              ///< Worker is processing a batch
static struct NmStatsResult *Results = NULL; ///< Computed results (output)
static int NResults = 0;               ///< Number of results
static bool HasResult = false;         ///< Results are waiting to be collected
static bool Shutdown = false;          ///< Ask the worker to exit

// ---------------------------------------------------------------------------

/**
 * config_free - Free a config snapshot's strings
 * @param c Config snapshot
 */
static void config_free(struct NmStatsConfig *c)
{
  if (!c)
    return;

  free(c->unread_tag);
  free(c->flagged_tag);
  free(c->config_file);
  free(c->config_profile);
  if (c->exclude_tags)
  {
    for (char **t = c->exclude_tags; *t; t++)
      free(*t);
    free(c->exclude_tags);
  }
  memset(c, 0, sizeof(*c));
}

/**
 * jobs_free - Free a job batch and its config snapshot
 * @param jobs  Array of jobs
 * @param njobs Number of jobs
 * @param cfg   Config snapshot (may be NULL)
 */
static void jobs_free(struct NmStatsJob *jobs, int njobs, struct NmStatsConfig *cfg)
{
  if (jobs)
  {
    for (int i = 0; i < njobs; i++)
    {
      free(jobs[i].db_query);
      free(jobs[i].db_filename);
    }
    free(jobs);
  }
  config_free(cfg);
}

// --- Worker-thread only: no NeoMutt shared state below this line -----------

/**
 * stats_db_open - Open a read-only Notmuch database (worker thread)
 * @param cfg      Config snapshot
 * @param filename Database path (may be NULL)
 * @retval ptr Notmuch database, or NULL on failure
 *
 * A thread-safe subset of nm_db_do_open(): it takes all config as parameters
 * and performs no logging or UI calls.
 */
static notmuch_database_t *stats_db_open(const struct NmStatsConfig *cfg, const char *filename)
{
  notmuch_database_t *db = NULL;
  int ct = 0;
  notmuch_status_t st = NOTMUCH_STATUS_SUCCESS;

  do
  {
#if LIBNOTMUCH_CHECK_VERSION(5, 4, 0)
    char *msg = NULL;
    st = notmuch_database_open_with_config(filename, NOTMUCH_DATABASE_MODE_READ_ONLY,
                                           cfg->config_file, cfg->config_profile,
                                           &db, &msg);
    if ((st == NOTMUCH_STATUS_NO_CONFIG) && cfg->config_file && (cfg->config_file[0] != '\0'))
    {
      free(msg);
      msg = NULL;
      st = notmuch_database_open_with_config(filename, NOTMUCH_DATABASE_MODE_READ_ONLY,
                                             "", NULL, &db, &msg);
    }
    free(msg);
#elif LIBNOTMUCH_CHECK_VERSION(4, 2, 0)
    char *msg = NULL;
    st = notmuch_database_open_verbose(filename, NOTMUCH_DATABASE_MODE_READ_ONLY, &db, &msg);
    free(msg);
#elif defined(NOTMUCH_API_3)
    st = notmuch_database_open(filename, NOTMUCH_DATABASE_MODE_READ_ONLY, &db);
#else
    db = notmuch_database_open(filename, NOTMUCH_DATABASE_MODE_READ_ONLY);
#endif

    if ((st == NOTMUCH_STATUS_FILE_ERROR) || db || (cfg->open_timeout <= 0) ||
        ((ct / 2) > cfg->open_timeout))
    {
      break;
    }

    const struct timespec ts = { 0, 500 * 1000 * 1000 }; // 0.5s
    nanosleep(&ts, NULL);
    ct++;
  } while (true);

  if (st != NOTMUCH_STATUS_SUCCESS)
    db = NULL;

  return db;
}

/**
 * stats_count - Count the results of a query (worker thread)
 * @param db      Notmuch database
 * @param qstr    Query string
 * @param exclude NULL-terminated exclude-tag array, or NULL
 * @param limit   Result limit (<= 0 for none)
 * @retval num Number of matching messages
 *
 * A thread-safe subset of count_query() + apply_exclude_tags().
 */
static unsigned int stats_count(notmuch_database_t *db, const char *qstr,
                                char *const *exclude, int limit)
{
  notmuch_query_t *q = notmuch_query_create(db, qstr);
  if (!q)
    return 0;

  if (exclude)
  {
    for (char *const *t = exclude; *t; t++)
      notmuch_query_add_tag_exclude(q, *t);
    notmuch_query_set_omit_excluded(q, 1);
  }

  unsigned int res = 0;
#if LIBNOTMUCH_CHECK_VERSION(5, 0, 0)
  if (notmuch_query_count_messages(q, &res) != NOTMUCH_STATUS_SUCCESS)
    res = 0;
#elif LIBNOTMUCH_CHECK_VERSION(4, 3, 0)
  if (notmuch_query_count_messages_st(q, &res) != NOTMUCH_STATUS_SUCCESS)
    res = 0;
#else
  res = notmuch_query_count_messages(q);
#endif
  notmuch_query_destroy(q);

  if ((limit > 0) && (res > (unsigned int) limit))
    res = (unsigned int) limit;

  return res;
}

/**
 * build_query - Wrap a base query with a tag filter (worker thread)
 * @param base Base query string
 * @param tag  Tag to require
 * @retval ptr Newly allocated query string (caller frees), or NULL
 */
static char *build_query(const char *base, const char *tag)
{
  const char *t = tag ? tag : "";
  int len = snprintf(NULL, 0, "( %s ) tag:%s", base, t);
  if (len < 0)
    return NULL;

  char *s = malloc((size_t) len + 1);
  if (!s)
    return NULL;

  snprintf(s, (size_t) len + 1, "( %s ) tag:%s", base, t);
  return s;
}

/**
 * stats_worker - Background worker main loop - Implements pthread start routine
 * @param arg Unused
 * @retval NULL Always
 */
static void *stats_worker(void *arg)
{
  (void) arg;

  while (true)
  {
    pthread_mutex_lock(&Mutex);
    while (!Shutdown && !HasJob)
      pthread_cond_wait(&Cond, &Mutex);

    if (Shutdown)
    {
      pthread_mutex_unlock(&Mutex);
      break;
    }

    // Take ownership of the batch
    struct NmStatsJob *jobs = Jobs;
    int njobs = NJobs;
    struct NmStatsConfig cfg = Cfg; // shallow copy; owns the strings now
    Jobs = NULL;
    NJobs = 0;
    memset(&Cfg, 0, sizeof(Cfg));
    HasJob = false;
    Busy = true;
    pthread_mutex_unlock(&Mutex);

    // --- Heavy lifting, no locks held, no shared NeoMutt state touched ---
    struct NmStatsResult *res = calloc((size_t) njobs, sizeof(*res));
    int nres = 0;
    notmuch_database_t *db = NULL;
    char *db_open_name = NULL;

    for (int i = 0; res && (i < njobs); i++)
    {
      struct NmStatsJob *j = &jobs[i];
      if (!j->db_query)
        continue;

      // Reuse the open handle when consecutive mailboxes share a database
      const bool same = (db_open_name == j->db_filename) ||
                        (db_open_name && j->db_filename &&
                         (strcmp(db_open_name, j->db_filename) == 0));
      if (!db || !same)
      {
        if (db)
          nm_db_free(db);
        db = stats_db_open(&cfg, j->db_filename);
        free(db_open_name);
        db_open_name = j->db_filename ? strdup(j->db_filename) : NULL;
      }
      if (!db)
        continue;

      struct NmStatsResult *r = &res[nres];
      r->mbox_id = j->mbox_id;
      r->msg_count = (int) stats_count(db, j->db_query, cfg.exclude_tags, j->limit);

      char *qs = build_query(j->db_query, cfg.unread_tag);
      if (qs)
      {
        r->msg_unread = (int) stats_count(db, qs, cfg.exclude_tags, j->limit);
        free(qs);
      }

      qs = build_query(j->db_query, cfg.flagged_tag);
      if (qs)
      {
        r->msg_flagged = (int) stats_count(db, qs, cfg.exclude_tags, j->limit);
        free(qs);
      }

      nres++;
    }

    if (db)
      nm_db_free(db);
    free(db_open_name);
    jobs_free(jobs, njobs, &cfg);

    // --- Post results back to the main thread ---
    pthread_mutex_lock(&Mutex);
    if (HasResult) // discard anything not yet collected (shouldn't happen)
    {
      free(Results);
      Results = NULL;
      NResults = 0;
    }
    Results = res;
    NResults = nres;
    HasResult = (res != NULL);
    Busy = false;
    pthread_mutex_unlock(&Mutex);
  }

  return NULL;
}

/**
 * worker_once_init - One-time worker thread startup - Implements pthread_once()
 */
static void worker_once_init(void)
{
  if (pthread_create(&Worker, NULL, stats_worker, NULL) == 0)
  {
    WorkerOk = true;
    WorkerStarted = true;
  }
}

/**
 * ensure_worker - Lazily start the worker thread
 * @retval true Worker is running
 */
static bool ensure_worker(void)
{
  pthread_once(&Once, worker_once_init);
  return WorkerOk;
}

// --- Main-thread config/job snapshotting -----------------------------------

/**
 * snapshot_exclude - Snapshot $nm_exclude_tags into an owned array
 * @param ex Value of $nm_exclude_tags (may be NULL)
 * @retval ptr  NULL-terminated array of strdup'd tags (caller frees)
 * @retval NULL $nm_exclude_tags is unset
 */
static char **snapshot_exclude(const char *ex)
{
  if (!ex)
    return NULL;

  struct NmTags tags = nm_tag_str_to_tags(ex);
  int n = ARRAY_SIZE(&tags.tags);
  char **out = calloc((size_t) n + 1, sizeof(char *));
  if (out)
  {
    const char **t = NULL;
    int i = 0;
    ARRAY_FOREACH(t, &tags.tags)
    {
      out[i++] = strdup(*t);
    }
    out[i] = NULL;
  }
  nm_tag_array_free(&tags);
  return out;
}

/**
 * snapshot_config - Snapshot the config the worker needs (main thread)
 * @param cfg Config snapshot to fill in
 */
static void snapshot_config(struct NmStatsConfig *cfg)
{
  memset(cfg, 0, sizeof(*cfg));

  const char *u = cs_subset_string(NeoMutt->sub, "nm_unread_tag");
  const char *f = cs_subset_string(NeoMutt->sub, "nm_flagged_tag");
  const char *ex = cs_subset_string(NeoMutt->sub, "nm_exclude_tags");

  cfg->unread_tag = u ? strdup(u) : NULL;
  cfg->flagged_tag = f ? strdup(f) : NULL;
  cfg->exclude_tags = snapshot_exclude(ex);
  cfg->db_limit = cs_subset_number(NeoMutt->sub, "nm_db_limit");
  cfg->open_timeout = cs_subset_number(NeoMutt->sub, "nm_open_timeout");

#if LIBNOTMUCH_CHECK_VERSION(5, 4, 0)
  const char *cfp = cs_subset_string(NeoMutt->sub, "nm_config_profile");
  cfg->config_profile = cfp ? strdup(cfp) : NULL;

  // Mirror get_nm_config_file(): NULL means "auto", "" means "no config file"
  const char *ncf = cs_subset_path(NeoMutt->sub, "nm_config_file");
  const char *use = NULL;
  if (!ncf)
    use = "";
  else if (!mutt_strn_equal(ncf, "auto", 4))
    use = ncf;
  cfg->config_file = use ? strdup(use) : NULL;
#endif
}

/**
 * snapshot_job - Snapshot one Notmuch mailbox into a job (main thread)
 * @param m   Mailbox
 * @param job Job to fill in
 * @param default_limit Fallback limit ($nm_db_limit)
 * @retval true Job is valid and ready to run
 *
 * Parses the mailbox URL exactly like nm_mbox_check_stats() does.
 */
static bool snapshot_job(struct Mailbox *m, struct NmStatsJob *job, int default_limit)
{
  memset(job, 0, sizeof(*job));

  struct Url *url = url_parse(mailbox_path(m));
  if (!url)
    return false;

  const char *db_query = NULL;
  int limit = default_limit;
  struct UrlQuery *item = NULL;
  STAILQ_FOREACH(item, &url->query_strings, entries)
  {
    if (item->value && mutt_str_equal(item->name, "query"))
      db_query = item->value;
    else if (item->value && mutt_str_equal(item->name, "limit"))
      mutt_str_atoi_full(item->value, &limit);
  }

  bool ok = false;
  if (db_query)
  {
    const char *fname = url->path;
    if (!fname)
      fname = nm_db_get_filename(m);

    job->mbox_id = m;
    job->db_query = strdup(db_query);
    job->db_filename = fname ? strdup(fname) : NULL;
    job->limit = limit;
    ok = true;
  }

  url_free(&url);
  return ok;
}

/**
 * find_live_mailbox - Find a Mailbox by pointer identity in the current list
 * @param ma Current mailbox list
 * @param id Opaque Mailbox pointer to match
 * @retval ptr Live Mailbox, or NULL if it no longer exists
 */
static struct Mailbox *find_live_mailbox(struct MailboxArray *ma, void *id)
{
  struct Mailbox **mp = NULL;
  ARRAY_FOREACH(mp, ma)
  {
    if (*mp == id)
      return *mp;
  }
  return NULL;
}

// --- Public API ------------------------------------------------------------

/**
 * nm_stats_async_available - Is the background worker usable?
 * @retval true The worker thread is running
 *
 * Starts the worker on first use.  If it returns false (e.g. pthread_create()
 * failed), callers should fall back to synchronous stats.
 */
bool nm_stats_async_available(void)
{
  return ensure_worker();
}

/**
 * nm_stats_async_busy - Is the worker processing or are results pending?
 * @retval true A dispatch is in flight or results await collection
 */
bool nm_stats_async_busy(void)
{
  if (!WorkerStarted)
    return false;

  pthread_mutex_lock(&Mutex);
  bool b = Busy || HasJob || HasResult;
  pthread_mutex_unlock(&Mutex);
  return b;
}

/**
 * nm_stats_async_dispatch - Queue stats work for a set of Notmuch mailboxes
 * @param mboxes Array of Notmuch Mailboxes
 * @param count  Number of mailboxes
 * @retval true  Work was queued
 * @retval false Nothing queued (worker busy, unavailable, or no valid jobs)
 *
 * Non-blocking.  Snapshots everything the worker needs and returns immediately.
 */
bool nm_stats_async_dispatch(struct Mailbox **mboxes, int count)
{
  if (!mboxes || (count <= 0))
    return false;

  if (!ensure_worker())
    return false;

  if (nm_stats_async_busy())
    return false;

  struct NmStatsConfig cfg = { 0 };
  snapshot_config(&cfg);

  struct NmStatsJob *jobs = calloc((size_t) count, sizeof(*jobs));
  if (!jobs)
  {
    config_free(&cfg);
    return false;
  }

  int njobs = 0;
  for (int i = 0; i < count; i++)
  {
    if (mboxes[i] && snapshot_job(mboxes[i], &jobs[njobs], cfg.db_limit))
      njobs++;
  }

  if (njobs == 0)
  {
    jobs_free(jobs, njobs, &cfg);
    return false;
  }

  pthread_mutex_lock(&Mutex);
  if (Busy || HasJob) // lost a race; back off
  {
    pthread_mutex_unlock(&Mutex);
    jobs_free(jobs, njobs, &cfg);
    return false;
  }
  Jobs = jobs;
  NJobs = njobs;
  Cfg = cfg; // ownership of strings transfers to the worker
  HasJob = true;
  pthread_cond_signal(&Cond);
  pthread_mutex_unlock(&Mutex);
  return true;
}

/**
 * nm_stats_async_apply - Apply any ready results to live Mailboxes
 * @retval num Number of Mailboxes updated
 *
 * Called on the main thread.  Copies the computed counts onto matching live
 * Mailboxes and fires an #NT_MAILBOX_CHANGE notification so the sidebar and
 * status line redraw.  Never reallocates the emails array.
 */
int nm_stats_async_apply(void)
{
  if (!WorkerStarted)
    return 0;

  pthread_mutex_lock(&Mutex);
  if (!HasResult)
  {
    pthread_mutex_unlock(&Mutex);
    return 0;
  }
  struct NmStatsResult *res = Results;
  int n = NResults;
  Results = NULL;
  NResults = 0;
  HasResult = false;
  pthread_mutex_unlock(&Mutex);

  int applied = 0;
  struct MailboxArray ma = neomutt_mailboxes_get(NeoMutt, MUTT_MAILBOX_ANY);
  for (int i = 0; i < n; i++)
  {
    struct Mailbox *m = find_live_mailbox(&ma, res[i].mbox_id);
    if (!m)
      continue;

    // An open Mailbox's counts belong to its view (and its emails array must
    // match msg_count); it may have been opened since the job was dispatched.
    if (m->opened > 0)
      continue;

    m->msg_count = res[i].msg_count;
    m->msg_unread = res[i].msg_unread;
    m->msg_flagged = res[i].msg_flagged;

    struct EventMailbox ev_m = { m };
    notify_send(m->notify, NT_MAILBOX, NT_MAILBOX_CHANGE, &ev_m);
    applied++;
  }
  ARRAY_FREE(&ma); // Clean up the ARRAY, but not the Mailboxes
  free(res);

  return applied;
}

/**
 * nm_stats_async_shutdown - Stop the worker thread and free resources
 *
 * Safe to call even if the worker was never started.
 */
void nm_stats_async_shutdown(void)
{
  if (!WorkerStarted)
    return;

  pthread_mutex_lock(&Mutex);
  Shutdown = true;
  pthread_cond_signal(&Cond);
  pthread_mutex_unlock(&Mutex);

  pthread_join(Worker, NULL);
  WorkerStarted = false;
  WorkerOk = false;

  jobs_free(Jobs, NJobs, &Cfg);
  Jobs = NULL;
  NJobs = 0;
  HasJob = false;

  free(Results);
  Results = NULL;
  NResults = 0;
  HasResult = false;
}
