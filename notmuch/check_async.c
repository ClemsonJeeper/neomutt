/**
 * @file
 * Background worker for the open Notmuch mailbox check
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
 * @page notmuch_check_async Background worker for the open Notmuch mailbox check
 *
 * Re-checking the open Notmuch mailbox means running the vfolder query and
 * inspecting every matching message (its filename and tags).  On a large
 * vfolder this is several seconds of Xapian/libnotmuch work, and it used to run
 * inline on the UI thread, freezing NeoMutt.
 *
 * This module runs that read-only enumeration on a background thread.  It
 * produces a plain-data snapshot of @ref NmCheckRecord entries (message-id,
 * filename, tags) which the main thread later applies to the mailbox with no
 * libnotmuch calls (see nm_apply_check_snapshot() in notmuch.c).
 *
 * The worker touches no NeoMutt state: it opens its own read-only database
 * handle and only calls libnotmuch and libc.
 */

#include "config.h"
#include <notmuch.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "private.h"

// --- Worker lifecycle ------------------------------------------------------

static pthread_t Worker;                                  ///< Worker thread handle
static pthread_mutex_t Mutex = PTHREAD_MUTEX_INITIALIZER; ///< Guards shared state
static pthread_cond_t Cond = PTHREAD_COND_INITIALIZER;    ///< Wakes the worker
static pthread_once_t Once = PTHREAD_ONCE_INIT;           ///< One-time startup
static bool WorkerOk = false;                             ///< Worker started ok
static bool WorkerStarted = false;                        ///< pthread_create() ok

// --- Shared state (guarded by Mutex) ---------------------------------------

static struct NmCheckJob Job = { 0 };                ///< Pending job (input)
static bool HasJob = false;                          ///< A job is waiting
static bool Busy = false;                            ///< Worker is running
static struct NmCheckSnapshot *Result = NULL;        ///< Computed snapshot (output)
static bool HasResult = false;                       ///< Result awaits collection
static bool Shutdown = false;                        ///< Ask the worker to exit

// ---------------------------------------------------------------------------

/**
 * nm_check_job_free - Free a job's owned strings
 * @param job Job (may be NULL)
 */
void nm_check_job_free(struct NmCheckJob *job)
{
  if (!job)
    return;

  free(job->db_filename);
  free(job->db_query);
  free(job->cfg_file);
  free(job->cfg_profile);
  if (job->exclude_tags)
  {
    for (char **t = job->exclude_tags; *t; t++)
      free(*t);
    free(job->exclude_tags);
  }
  memset(job, 0, sizeof(*job));
}

/**
 * nm_check_snapshot_free - Free a snapshot and its records
 * @param pptr Pointer to the snapshot pointer (set to NULL)
 */
void nm_check_snapshot_free(struct NmCheckSnapshot **pptr)
{
  if (!pptr || !*pptr)
    return;

  struct NmCheckSnapshot *snap = *pptr;
  if (snap->recs)
  {
    for (int i = 0; i < snap->nrecs; i++)
    {
      free(snap->recs[i].msgid);
      free(snap->recs[i].path);
      free(snap->recs[i].tags);
    }
    free(snap->recs);
  }
  free(snap);
  *pptr = NULL;
}

// --- Worker-thread only ----------------------------------------------------

/**
 * check_db_open - Open a read-only Notmuch database (worker thread)
 * @param job Job carrying the open parameters
 * @retval ptr Notmuch database, or NULL
 *
 * A thread-safe subset of nm_db_do_open(): all config is passed in, and it
 * performs no logging or UI calls.
 */
static notmuch_database_t *check_db_open(const struct NmCheckJob *job)
{
  notmuch_database_t *db = NULL;
  int ct = 0;
  notmuch_status_t st = NOTMUCH_STATUS_SUCCESS;

  do
  {
#if LIBNOTMUCH_CHECK_VERSION(5, 4, 0)
    char *msg = NULL;
    st = notmuch_database_open_with_config(job->db_filename, NOTMUCH_DATABASE_MODE_READ_ONLY,
                                           job->cfg_file, job->cfg_profile, &db, &msg);
    if ((st == NOTMUCH_STATUS_NO_CONFIG) && job->cfg_file && (job->cfg_file[0] != '\0'))
    {
      free(msg);
      msg = NULL;
      st = notmuch_database_open_with_config(job->db_filename, NOTMUCH_DATABASE_MODE_READ_ONLY,
                                             "", NULL, &db, &msg);
    }
    free(msg);
#elif LIBNOTMUCH_CHECK_VERSION(4, 2, 0)
    char *msg = NULL;
    st = notmuch_database_open_verbose(job->db_filename, NOTMUCH_DATABASE_MODE_READ_ONLY, &db, &msg);
    free(msg);
#elif defined(NOTMUCH_API_3)
    st = notmuch_database_open(job->db_filename, NOTMUCH_DATABASE_MODE_READ_ONLY, &db);
#else
    db = notmuch_database_open(job->db_filename, NOTMUCH_DATABASE_MODE_READ_ONLY);
#endif

    if ((st == NOTMUCH_STATUS_FILE_ERROR) || db || (job->open_timeout <= 0) ||
        ((ct / 2) > job->open_timeout))
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
 * check_last_filename - Get a message's last filename (worker thread)
 * @param msg Notmuch message
 * @retval ptr Newly allocated path (caller frees), or NULL
 */
static char *check_last_filename(notmuch_message_t *msg)
{
  const char *name = NULL;
  for (notmuch_filenames_t *ls = notmuch_message_get_filenames(msg);
       ls && notmuch_filenames_valid(ls); notmuch_filenames_move_to_next(ls))
  {
    name = notmuch_filenames_get(ls);
  }
  return name ? strdup(name) : NULL;
}

/**
 * check_join_tags - Join a message's tags into a space-separated string (worker thread)
 * @param msg Notmuch message
 * @retval ptr Newly allocated string (caller frees), or NULL
 */
static char *check_join_tags(notmuch_message_t *msg)
{
  char *out = NULL;
  size_t len = 0;

  for (notmuch_tags_t *tags = notmuch_message_get_tags(msg);
       tags && notmuch_tags_valid(tags); notmuch_tags_move_to_next(tags))
  {
    const char *t = notmuch_tags_get(tags);
    if (!t || (t[0] == '\0'))
      continue;

    size_t tlen = strlen(t);
    size_t need = len + (len ? 1 : 0) + tlen + 1;
    char *tmp = realloc(out, need);
    if (!tmp)
      break;
    out = tmp;
    if (len)
      out[len++] = ' ';
    memcpy(out + len, t, tlen);
    len += tlen;
    out[len] = '\0';
  }

  return out;
}

/**
 * check_hash_str - Fold a string into an FNV-1a hash (worker thread)
 * @param h   Running hash
 * @param str String (may be NULL)
 * @retval num New hash
 */
static uint64_t check_hash_str(uint64_t h, const char *str)
{
  if (str)
  {
    for (const unsigned char *p = (const unsigned char *) str; *p; p++)
    {
      h ^= *p;
      h *= 1099511628211ULL;
    }
  }
  h ^= 0xff; // field separator
  h *= 1099511628211ULL;
  return h;
}

/**
 * check_enumerate - Run the query and build a snapshot (worker thread)
 * @param job Job describing the query
 * @retval ptr Newly allocated snapshot (caller frees), or NULL
 */
static struct NmCheckSnapshot *check_enumerate(const struct NmCheckJob *job)
{
  struct NmCheckSnapshot *snap = calloc(1, sizeof(*snap));
  if (!snap)
    return NULL;
  snap->mbox_id = job->mbox_id;
  snap->gen = job->gen;
  snap->stamp = job->stamp;

  if (!job->db_query)
    return snap; // ok == false

  notmuch_database_t *db = check_db_open(job);
  if (!db)
    return snap;

  notmuch_query_t *q = notmuch_query_create(db, job->db_query);
  if (!q)
  {
    nm_db_free(db);
    return snap;
  }

  if (job->exclude_tags)
  {
    for (char **t = job->exclude_tags; *t; t++)
      notmuch_query_add_tag_exclude(q, *t);
    notmuch_query_set_omit_excluded(q, 1);
  }
  notmuch_query_set_sort(q, NOTMUCH_SORT_NEWEST_FIRST);

  notmuch_messages_t *msgs = NULL;
#if LIBNOTMUCH_CHECK_VERSION(5, 0, 0)
  if (notmuch_query_search_messages(q, &msgs) != NOTMUCH_STATUS_SUCCESS)
    msgs = NULL;
#elif LIBNOTMUCH_CHECK_VERSION(4, 3, 0)
  if (notmuch_query_search_messages_st(q, &msgs) != NOTMUCH_STATUS_SUCCESS)
    msgs = NULL;
#else
  msgs = notmuch_query_search_messages(q);
#endif

  int cap = 0;
  int n = 0;
  uint64_t hash = 14695981039346656037ULL;
  const int limit = job->limit;

  for (; msgs && notmuch_messages_valid(msgs) && ((limit == 0) || (n < limit));
       notmuch_messages_move_to_next(msgs))
  {
    notmuch_message_t *msg = notmuch_messages_get(msgs);
    if (!msg)
      continue;

    const char *id = notmuch_message_get_message_id(msg);
    if (id)
    {
      if (n == cap)
      {
        int newcap = cap ? (cap * 2) : 256;
        struct NmCheckRecord *tmp = realloc(snap->recs, (size_t) newcap * sizeof(*tmp));
        if (!tmp)
        {
          notmuch_message_destroy(msg);
          break;
        }
        snap->recs = tmp;
        cap = newcap;
      }
      snap->recs[n].msgid = strdup(id);
      snap->recs[n].path = check_last_filename(msg);
      snap->recs[n].tags = check_join_tags(msg);
      hash = check_hash_str(hash, id);
      hash = check_hash_str(hash, snap->recs[n].path);
      hash = check_hash_str(hash, snap->recs[n].tags);
      n++;
    }

    notmuch_message_destroy(msg);
  }

  snap->nrecs = n;
  snap->hash = hash;
  snap->ok = true;

  notmuch_query_destroy(q);
  nm_db_free(db);
  return snap;
}

/**
 * check_worker - Background worker main loop - Implements pthread start routine
 * @param arg Unused
 * @retval NULL Always
 */
static void *check_worker(void *arg)
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

    struct NmCheckJob job = Job; // shallow copy; owns the strings now
    memset(&Job, 0, sizeof(Job));
    HasJob = false;
    Busy = true;
    pthread_mutex_unlock(&Mutex);

    struct NmCheckSnapshot *snap = check_enumerate(&job);
    nm_check_job_free(&job);

    pthread_mutex_lock(&Mutex);
    if (HasResult) // discard anything not yet collected (shouldn't happen)
      nm_check_snapshot_free(&Result);
    Result = snap;
    HasResult = (snap != NULL);
    Busy = false;
    pthread_mutex_unlock(&Mutex);
  }

  return NULL;
}

/**
 * check_once_init - One-time worker startup - Implements pthread_once()
 */
static void check_once_init(void)
{
  if (pthread_create(&Worker, NULL, check_worker, NULL) == 0)
  {
    WorkerOk = true;
    WorkerStarted = true;
  }
}

// --- Public API (private.h) ------------------------------------------------

/**
 * nm_check_async_available - Is the background worker usable?
 * @retval true The worker thread is running
 */
bool nm_check_async_available(void)
{
  pthread_once(&Once, check_once_init);
  return WorkerOk;
}

/**
 * nm_check_async_busy - Is the worker running or is a result pending?
 * @retval true A job is in flight or a snapshot awaits collection
 */
bool nm_check_async_busy(void)
{
  if (!WorkerStarted)
    return false;

  pthread_mutex_lock(&Mutex);
  bool b = Busy || HasJob || HasResult;
  pthread_mutex_unlock(&Mutex);
  return b;
}

/**
 * nm_check_async_submit - Queue a mailbox enumeration
 * @param job Job to run (ownership of its strings transfers on success)
 * @retval true  Job was queued
 * @retval false Not queued (worker busy or unavailable); caller still owns job
 *
 * Non-blocking.
 */
bool nm_check_async_submit(struct NmCheckJob *job)
{
  if (!job || !nm_check_async_available())
    return false;

  pthread_mutex_lock(&Mutex);
  if (Busy || HasJob || HasResult)
  {
    pthread_mutex_unlock(&Mutex);
    return false;
  }
  Job = *job; // take ownership of the strings
  memset(job, 0, sizeof(*job));
  HasJob = true;
  pthread_cond_signal(&Cond);
  pthread_mutex_unlock(&Mutex);
  return true;
}

/**
 * nm_check_async_collect - Take the ready snapshot, if any
 * @retval ptr  Snapshot (caller must free with nm_check_snapshot_free())
 * @retval NULL No snapshot ready
 */
struct NmCheckSnapshot *nm_check_async_collect(void)
{
  if (!WorkerStarted)
    return NULL;

  pthread_mutex_lock(&Mutex);
  struct NmCheckSnapshot *snap = NULL;
  if (HasResult)
  {
    snap = Result;
    Result = NULL;
    HasResult = false;
  }
  pthread_mutex_unlock(&Mutex);
  return snap;
}

/**
 * nm_check_async_shutdown - Stop the worker thread and free resources
 */
void nm_check_async_shutdown(void)
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

  nm_check_job_free(&Job);
  HasJob = false;
  nm_check_snapshot_free(&Result);
  HasResult = false;
}
