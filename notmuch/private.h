/**
 * @file
 * Notmuch private types
 *
 * @authors
 * Copyright (C) 2018-2021 Richard Russon <rich@flatcap.org>
 * Copyright (C) 2021 Austin Ray <austin@austinray.io>
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

#ifndef MUTT_NOTMUCH_PRIVATE_H
#define MUTT_NOTMUCH_PRIVATE_H

#include <notmuch.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>

struct Mailbox;

#ifdef LIBNOTMUCH_CHECK_VERSION
#undef LIBNOTMUCH_CHECK_VERSION
#endif

#ifndef HAVE_NOTMUCH_DATABASE_INDEX_FILE
#define HAVE_NOTMUCH_DATABASE_INDEX_FILE 0
#endif

#ifndef HAVE_NOTMUCH_DATABASE_OPEN_WITH_CONFIG
#define HAVE_NOTMUCH_DATABASE_OPEN_WITH_CONFIG 0
#endif

/*
 * The definition in <notmuch.h> is broken.
 *
 * Corrects for libnotmuch releases with missing version bumps:
 * - libnotmuch 5.4 released with notmuch 0.32. notmuch 0.32.3 fixed version.
 * - libnotmuch 5.1 released with notmuch 0.26. notmuch 0.26.1 fixed version.
 */
#define LIBNOTMUCH_CHECK_VERSION(major, minor, micro)                              \
  ((major) == 5 && (minor) == 4 && HAVE_NOTMUCH_DATABASE_OPEN_WITH_CONFIG)      || \
  ((major) == 5 && (minor) == 1 && HAVE_NOTMUCH_DATABASE_INDEX_FILE)            || \
  ((LIBNOTMUCH_MAJOR_VERSION > (major) ||                                          \
   (LIBNOTMUCH_MAJOR_VERSION == (major) && LIBNOTMUCH_MINOR_VERSION > (minor))  || \
   (LIBNOTMUCH_MAJOR_VERSION == (major) &&                                         \
    LIBNOTMUCH_MINOR_VERSION == (minor) && LIBNOTMUCH_MICRO_VERSION >= (micro))))

extern const char NmUrlProtocol[];
extern const int NmUrlProtocolLen;

/**
 * struct NmCheckRecord - Read-only snapshot of one Notmuch message
 *
 * Collected by the background worker; all fields are owned by the record.
 */
struct NmCheckRecord
{
  char *msgid; ///< Notmuch message-id
  char *path;  ///< Last known filename
  char *tags;  ///< Space-separated tags
};

/**
 * struct NmCheckSnapshot - Result of a background mailbox enumeration
 */
struct NmCheckSnapshot
{
  void *mbox_id;                ///< Opaque Mailbox pointer (for matching only)
  struct NmCheckRecord *recs;   ///< Array of message records, in query order
  int nrecs;                    ///< Number of records
  bool ok;                      ///< Enumeration completed successfully
  uint64_t hash;                ///< Hash of every record (to detect an unchanged result)
  unsigned int gen;             ///< Local-change generation when the job was dispatched
  time_t stamp;                 ///< Time the job was dispatched (lower bound of the snapshot's age)
};

/**
 * struct NmCheckJob - Inputs for a background mailbox enumeration
 *
 * Snapshotted on the main thread and handed to the worker, which owns and frees
 * all the strings.
 */
struct NmCheckJob
{
  void *mbox_id;        ///< Opaque Mailbox pointer (for matching only)
  char *db_filename;    ///< Notmuch database path (may be NULL = auto)
  char *db_query;       ///< Effective query string
  int limit;            ///< Result limit (0 = none)
  char **exclude_tags;  ///< NULL-terminated exclude-tag array, or NULL
  char *cfg_file;       ///< Notmuch config file (NULL = auto, "" = none)
  char *cfg_profile;    ///< Notmuch config profile
  int open_timeout;     ///< Database open timeout (seconds)
  unsigned int gen;     ///< Local-change generation when the job was dispatched
  time_t stamp;         ///< Time the job was dispatched
};

notmuch_database_t *nm_db_do_open     (const char *filename, bool writable, bool verbose);
void                nm_db_free        (notmuch_database_t *db);
const char *        nm_db_get_filename(struct Mailbox *m);
int                 nm_db_get_mtime   (struct Mailbox *m, time_t *mtime);
notmuch_database_t *nm_db_get         (struct Mailbox *m, bool writable);
bool                nm_db_is_longrun  (struct Mailbox *m);
int                 nm_db_release     (struct Mailbox *m);
int                 nm_db_trans_begin (struct Mailbox *m);
int                 nm_db_trans_end   (struct Mailbox *m);

bool                     nm_check_async_available(void);
bool                     nm_check_async_busy     (void);
struct NmCheckSnapshot * nm_check_async_collect  (void);
void                     nm_check_async_shutdown (void);
bool                     nm_check_async_submit    (struct NmCheckJob *job);
void                     nm_check_job_free        (struct NmCheckJob *job);
void                     nm_check_snapshot_free   (struct NmCheckSnapshot **pptr);

#endif /* MUTT_NOTMUCH_PRIVATE_H */
