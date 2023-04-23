/***************************************************************************
 *   Copyright (C) 2020 by Kyle Hayes                                      *
 *   Author Kyle Hayes  kyle.hayes@gmail.com                               *
 *                                                                         *
 * This software is available under either the Mozilla Public License      *
 * version 2.0 or the GNU LGPL version 2 (or later) license, whichever     *
 * you choose.                                                             *
 *                                                                         *
 * MPL 2.0:                                                                *
 *                                                                         *
 *   This Source Code Form is subject to the terms of the Mozilla Public   *
 *   License, v. 2.0. If a copy of the MPL was not distributed with this   *
 *   file, You can obtain one at http://mozilla.org/MPL/2.0/.              *
 *                                                                         *
 *                                                                         *
 * LGPL 2:                                                                 *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU Library General Public License as       *
 *   published by the Free Software Foundation; either version 2 of the    *
 *   License, or (at your option) any later version.                       *
 *                                                                         *
 *   This program is distributed in the hope that it will be useful,       *
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of        *
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         *
 *   GNU General Public License for more details.                          *
 *                                                                         *
 *   You should have received a copy of the GNU Library General Public     *
 *   License along with this program; if not, write to the                 *
 *   Free Software Foundation, Inc.,                                       *
 *   59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.             *
 ***************************************************************************/

#include "libplctag.h"
#include "libplctag_internal.h"

#ifndef __PLATFORM_C__
#define __PLATFORM_C__

#ifdef WIN32

#define _WINSOCKAPI_
#include <windows.h>
#include <tchar.h>
#include <strsafe.h>
#include <io.h>
#include <Winsock2.h>
#include <Ws2tcpip.h>
#include <timeapi.h>
#include <string.h>
#include <stdlib.h>
#include <winnt.h>
#include <errno.h>
#include <math.h>
#include <process.h>
#include <time.h>
#include <stdio.h>

#define WINDOWS_REQUESTED_TIMER_PERIOD_MS ((unsigned int)4)



/***************************************************************************
 ******************************* Memory ************************************
 **************************************************************************/



/*
 * mem_alloc
 *
 * This is a wrapper around the platform's memory allocation routine.
 * It will zero out memory before returning it.
 *
 * It will return NULL on failure.
 */
void *mem_alloc(int size)
{
    if(size <= 0) {
        pdebug(DEBUG_WARN, "Allocation size must be greater than zero bytes!");
        return NULL;
    }

    return calloc(size, 1);
}



/*
 * mem_realloc
 *
 * This is a wrapper around the platform's memory re-allocation routine.
 *
 * It will return NULL on failure.
 */
void *mem_realloc(void *orig, int size)
{
    if(size <= 0) {
        pdebug(DEBUG_WARN, "New allocation size must be greater than zero bytes!");
        return NULL;
    }

    return realloc(orig, (size_t)(ssize_t)size);
}





/*
 * mem_free
 *
 * Free the allocated memory passed in.  If the passed pointer is
 * null, do nothing.
 */
void mem_free(const void *mem)
{
    if(mem) {
        free((void *)mem);
    }
}




/*
 * mem_set
 *
 * set memory to the passed argument.
 */
void mem_set(void *dest, int c, int size)
{
    if(!dest) {
        pdebug(DEBUG_WARN, "Destination pointer is NULL!");
        return;
    }

    if(size <= 0) {
        pdebug(DEBUG_WARN, "Size to set must be a positive number!");
        return;
    }

    memset(dest, c, (size_t)(ssize_t)size);
}





/*
 * mem_copy
 *
 * copy memory from one pointer to another for the passed number of bytes.
 */
void mem_copy(void *dest, void *src, int size)
{
    if(!dest) {
        pdebug(DEBUG_WARN, "Destination pointer is NULL!");
        return;
    }

    if(!src) {
        pdebug(DEBUG_WARN, "Source pointer is NULL!");
        return;
    }

    if(size < 0) {
        pdebug(DEBUG_WARN, "Size to copy must be a positive number!");
        return;
    }

    if(size == 0) {
        /* nothing to do. */
        return;
    }

    memcpy(dest, src, (size_t)(ssize_t)size);
}





/*
 * mem_move
 *
 * move memory from one pointer to another for the passed number of bytes.
 */
void mem_move(void *dest, void *src, int size)
{
    if(!dest) {
        pdebug(DEBUG_WARN, "Destination pointer is NULL!");
        return;
    }

    if(!src) {
        pdebug(DEBUG_WARN, "Source pointer is NULL!");
        return;
    }

    if(size < 0) {
        pdebug(DEBUG_WARN, "Size to move must be a positive number!");
        return;
    }

    if(size == 0) {
        /* nothing to do. */
        return;
    }

    memmove(dest, src, (size_t)(ssize_t)size);
}





int mem_cmp(void *src1, int src1_size, void *src2, int src2_size)
{
    if(!src1 || src1_size <= 0) {
        if(!src2 || src2_size <= 0) {
            /* both are NULL or zero length, but that is "equal" for our purposes. */
            return 0;
        } else {
            /* first one is "less" than second. */
            return -1;
        }
    } else {
        if(!src2 || src2_size <= 0) {
            /* first is "greater" than second */
            return 1;
        } else {
            /* both pointers are non-NULL and the lengths are positive. */

            /* short circuit the comparison if the blocks are different lengths */
            if(src1_size != src2_size) {
                return (src1_size - src2_size);
            }

            return memcmp(src1, src2, src1_size);
        }
    }
}





/***************************************************************************
 ******************************* Strings ***********************************
 **************************************************************************/


/*
 * str_cmp
 *
 * Return -1, 0, or 1 depending on whether the first string is "less" than the
 * second, the same as the second, or "greater" than the second.  This routine
 * just passes through to POSIX strcmp.
 *
 * We must handle some edge cases here due to wrappers.   We could get a NULL
 * pointer or a zero-length string for either argument.
 */
int str_cmp(const char *first, const char *second)
{
    int first_zero = !str_length(first);
    int second_zero = !str_length(second);

    if(first_zero) {
        if(second_zero) {
            pdebug(DEBUG_DETAIL, "NULL or zero length strings passed.");
            return 0;
        } else {
            /* first is "less" than second. */
            return -1;
        }
    } else {
        if(second_zero) {
            /* first is "more" than second. */
            return 1;
        } else {
            /* both are non-zero length. */
            return strcmp(first, second);
        }
    }
}




/*
 * str_cmp_i
 *
 * Returns -1, 0, or 1 depending on whether the first string is "less" than the
 * second, the same as the second, or "greater" than the second.  The comparison
 * is done case insensitive.
 *
 * Handle the usual edge cases because Microsoft appears not to.
 */
int str_cmp_i(const char *first, const char *second)
{
    int first_zero = !str_length(first);
    int second_zero = !str_length(second);

    if(first_zero) {
        if(second_zero) {
            pdebug(DEBUG_DETAIL, "NULL or zero length strings passed.");
            return 0;
        } else {
            /* first is "less" than second. */
            return -1;
        }
    } else {
        if(second_zero) {
            /* first is "more" than second. */
            return 1;
        } else {
            /* both are non-zero length. */
            return _stricmp(first,second);
        }
    }
}


/*
 * str_cmp_i_n
 *
 * Returns -1, 0, or 1 depending on whether the first string is "less" than the
 * second, the same as the second, or "greater" than the second.  The comparison
 * is done case insensitive.   Compares only the first count characters.
 *
 * It just passes this through to Windows stricmp.
 */
int str_cmp_i_n(const char *first, const char *second, int count)
{
    int first_zero = !str_length(first);
    int second_zero = !str_length(second);

    if(count < 0) {
        pdebug(DEBUG_WARN, "Illegal negative count!");
        return -1;
    }

    if(count == 0) {
        pdebug(DEBUG_DETAIL, "Called with comparison count of zero!");
        return 0;
    }

    if(first_zero) {
        if(second_zero) {
            pdebug(DEBUG_DETAIL, "NULL or zero length strings passed.");
            return 0;
        } else {
            /* first is "less" than second. */
            return -1;
        }
    } else {
        if(second_zero) {
            /* first is "more" than second. */
            return 1;
        } else {
            /* both are non-zero length. */
            return _strnicmp(first, second, (size_t)(unsigned int)count);
        }
    }
}




/*
 * str_str_cmp_i
 *
 * Returns a pointer to the location of the needle string in the haystack string
 * or NULL if there is no match.  The comparison is done case-insensitive.
 *
 * Handle the usual edge cases.
 */


/*
 * grabbed from Apple open source, 2021/09/06, KRH.  Note public domain license.
 *
 * Modified to rename a few things and to add some checks and debugging output.
 */

/* +++Date last modified: 05-Jul-1997 */
/* $Id: stristr.c,v 1.5 2005/03/05 00:37:19 dasenbro Exp $ */

/*
** Designation:  StriStr
**
** Call syntax:  char *stristr(char *String, char *Pattern)
**
** Description:  This function is an ANSI version of strstr() with
**               case insensitivity.
**
** Return item:  char *pointer if Pattern is found in String, else
**               pointer to 0
**
** Rev History:  07/04/95  Bob Stout  ANSI-fy
**               02/03/94  Fred Cole  Original
**
** Hereby donated to public domain.
**
** Modified for use with libcyrus by Ken Murchison 06/01/00.
*/


char* str_str_cmp_i(const char* haystack, const char* needle)
{
    char* nptr, * hptr, * start;
    int haystack_len = str_length(haystack);
    int needle_len = str_length(needle);

    if (!haystack_len) {
        pdebug(DEBUG_DETAIL, "Haystack string is NULL or zero length.");
        return NULL;
    }

    if (!needle_len) {
        pdebug(DEBUG_DETAIL, "Needle string is NULL or zero length.");
        return NULL;
    }

    if (haystack_len < needle_len) {
        pdebug(DEBUG_DETAIL, "Needle string is longer than haystack string.");
        return NULL;
    }

    /* while haystack length not shorter than needle length */
    for (start = (char*)haystack, nptr = (char*)needle; haystack_len >= needle_len; start++, haystack_len--) {
        /* find start of needle in haystack */
        while (toupper(*start) != toupper(*needle)) {
            start++;
            haystack_len--;

            /* if needle longer than haystack */

            if (haystack_len < needle_len) {
                return(NULL);
            }
        }

        hptr = start;
        nptr = (char*)needle;

        while (toupper(*hptr) == toupper(*nptr)) {
            hptr++;
            nptr++;

            /* if end of needle then needle was found */
            if ('\0' == *nptr) {
                return (start);
            }
        }
    }

    return(NULL);
}


/*
 * str_copy
 *
 * Returns
 */
int str_copy(char *dst, int dst_size, const char *src)
{
    if (!dst) {
        pdebug(DEBUG_WARN, "Destination string pointer is NULL!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if (!src) {
        pdebug(DEBUG_WARN, "Source string pointer is NULL!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(dst_size <= 0) {
        pdebug(DEBUG_WARN, "Destination size is negative or zero!");
        return PLCTAG_ERR_TOO_SMALL;
    }


    /* FIXME - if there is not enough room, truncate the string. */
    strncpy_s(dst, dst_size, src, _TRUNCATE);

    return 0;
}


/*
 * str_length
 *
 * Return the length of the string.  If a null pointer is passed, return
 * null.
 */
int str_length(const char *str)
{
    if(!str) {
        return 0;
    }

    return (int)strlen(str);
}




/*
 * str_dup
 *
 * Copy the passed string and return a pointer to the copy.
 * The caller is responsible for freeing the memory.
 */
char *str_dup(const char *str)
{
    if(!str) {
        return NULL;
    }

    return _strdup(str);
}



/*
 * str_to_int
 *
 * Convert the characters in the passed string into
 * an int.  Return an int in integer in the passed
 * pointer and a status from the function.
 */
int str_to_int(const char *str, int *val)
{
    char *endptr;
    long int tmp_val;

    tmp_val = strtol(str,&endptr,0);

    if (errno == ERANGE && (tmp_val == LONG_MAX || tmp_val == LONG_MIN)) {
        /*pdebug("strtol returned %ld with errno %d",tmp_val, errno);*/
        return -1;
    }

    if (endptr == str) {
        return -1;
    }

    /* FIXME - this will truncate long values. */
    *val = (int)tmp_val;

    return 0;
}


int str_to_float(const char *str, float *val)
{
    char *endptr;
    double tmp_val_d;
    float tmp_val;

    /* Windows does not have strtof() */
    tmp_val_d = strtod(str,&endptr);

    if (errno == ERANGE && (tmp_val_d == HUGE_VAL || tmp_val_d == -HUGE_VAL || tmp_val_d == (double)0.0)) {
        return -1;
    }

    if (endptr == str) {
        return -1;
    }

    /* FIXME - this will truncate long values. */
    tmp_val = (float)tmp_val_d;
    *val = tmp_val;

    return 0;
}


char **str_split(const char *str, const char *sep)
{
    int sub_str_count=0;
    int size = 0;
    const char *sub;
    const char *tmp;
    char **res;

    /* first, count the sub strings */
    tmp = str;
    sub = strstr(tmp,sep);

    while(sub && *sub) {
        /* separator could be at the front, ignore that. */
        if(sub != tmp) {
            sub_str_count++;
        }

        tmp = sub + str_length(sep);
        sub = strstr(tmp,sep);
    }

    if(tmp && *tmp && (!sub || !*sub))
        sub_str_count++;

    /* calculate total size for string plus pointers */
    size = sizeof(char *)*(sub_str_count+1)+str_length(str)+1;

    /* allocate enough memory */
    res = (char**)mem_alloc(size);
    if(!res)
        return NULL;

    /* calculate the beginning of the string */
    tmp = (char *)res + sizeof(char *)*(sub_str_count+1);

    /* copy the string into the new buffer past the first part with the array of char pointers. */
    str_copy((char *)tmp, (int)(size - ((char*)tmp - (char*)res)), str);

    /* set up the pointers */
    sub_str_count=0;
    sub = strstr(tmp,sep);
    while(sub && *sub) {
        /* separator could be at the front, ignore that. */
        if(sub != tmp) {
            /* store the pointer */
            res[sub_str_count] = (char *)tmp;

            sub_str_count++;
        }

        /* zero out the separator chars */
        mem_set((char*)sub,0,str_length(sep));

        /* point past the separator (now zero) */
        tmp = sub + str_length(sep);

        /* find the next separator */
        sub = strstr(tmp,sep);
    }

    /* if there is a chunk at the end, store it. */
    if(tmp && *tmp && (!sub || !*sub)) {
        res[sub_str_count] = (char*)tmp;
    }

    return res;
}


char *str_concat_impl(int num_args, ...)
{
    va_list arg_list;
    int total_length = 0;
    char *result = NULL;
    char *tmp = NULL;

    /* first loop to find the length */
    va_start(arg_list, num_args);
    for(int i=0; i < num_args; i++) {
        tmp = va_arg(arg_list, char *);
        if(tmp) {
            total_length += str_length(tmp);
        }
    }
    va_end(arg_list);

    /* make a buffer big enough */
    total_length += 1;

    result = (char*)mem_alloc(total_length);
    if(!result) {
        pdebug(DEBUG_ERROR,"Unable to allocate new string buffer!");
        return NULL;
    }

    /* loop to copy the strings */
    result[0] = 0;
    va_start(arg_list, num_args);
    for(int i=0; i < num_args; i++) {
        tmp = va_arg(arg_list, char *);
        if(tmp) {
            int len = str_length(result);
            str_copy(&result[len], total_length - len, tmp);
        }
    }
    va_end(arg_list);

    return result;
}




/***************************************************************************
 ******************************* Mutexes ***********************************
 **************************************************************************/

struct mutex_t {
    HANDLE h_mutex;
    int initialized;
};


int mutex_create(mutex_p *m)
{
    pdebug(DEBUG_DETAIL, "Starting.");

    if(*m) {
        pdebug(DEBUG_WARN, "Called with non-NULL pointer!");
    }

    *m = (struct mutex_t *)mem_alloc(sizeof(struct mutex_t));
    if(! *m) {
        pdebug(DEBUG_WARN, "null mutex pointer!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* set up the mutex */
    (*m)->h_mutex = CreateMutex(
                        NULL,                   /* default security attributes  */
                        FALSE,                  /* initially not owned          */
                        NULL);                  /* unnamed mutex                */

    if(!(*m)->h_mutex) {
        mem_free(*m);
        *m = NULL;
        pdebug(DEBUG_WARN, "Error initializing mutex!");
        return PLCTAG_ERR_MUTEX_INIT;
    }

    (*m)->initialized = 1;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}



int mutex_lock_impl(const char *func, int line, mutex_p m)
{
    DWORD dwWaitResult = 0;

    pdebug(DEBUG_SPEW,"locking mutex %p, called from %s:%d.", m, func, line);

    if(!m) {
        pdebug(DEBUG_WARN, "null mutex pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!m->initialized) {
        return PLCTAG_ERR_MUTEX_INIT;
    }

    dwWaitResult = ~WAIT_OBJECT_0;

    /* FIXME - This will potentially hang forever! */
    while(dwWaitResult != WAIT_OBJECT_0) {
        dwWaitResult = WaitForSingleObject(m->h_mutex,INFINITE);
    }

    return PLCTAG_STATUS_OK;
}



int mutex_try_lock_impl(const char *func, int line, mutex_p m)
{
    DWORD dwWaitResult = 0;

    pdebug(DEBUG_SPEW,"trying to lock mutex %p, called from %s:%d.", m, func, line);

    if(!m) {
        pdebug(DEBUG_WARN, "null mutex pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!m->initialized) {
        return PLCTAG_ERR_MUTEX_INIT;
    }

    dwWaitResult = WaitForSingleObject(m->h_mutex, 0);
    if(dwWaitResult == WAIT_OBJECT_0) {
        /* we got the lock */
        return PLCTAG_STATUS_OK;
    } else {
        return PLCTAG_ERR_MUTEX_LOCK;
    }
}



int mutex_unlock_impl(const char *func, int line, mutex_p m)
{
    pdebug(DEBUG_SPEW,"unlocking mutex %p, called from %s:%d.", m, func, line);

    if(!m) {
        pdebug(DEBUG_WARN,"null mutex pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!m->initialized) {
        return PLCTAG_ERR_MUTEX_INIT;
    }

    if(!ReleaseMutex(m->h_mutex)) {
        /*pdebug("error unlocking mutex.");*/
        return PLCTAG_ERR_MUTEX_UNLOCK;
    }

    //pdebug("Done.");

    return PLCTAG_STATUS_OK;
}




int mutex_destroy(mutex_p *m)
{
    pdebug(DEBUG_DETAIL,"destroying mutex %p", m);

    if(!m || !*m) {
        pdebug(DEBUG_WARN, "null mutex pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    CloseHandle((*m)->h_mutex);

    mem_free(*m);

    *m = NULL;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}





/***************************************************************************
 ******************************* Threads ***********************************
 **************************************************************************/

struct thread_t {
    HANDLE h_thread;
    int initialized;
};


/*
 * thread_create()
 *
 * Start up a new thread.  This allocates the thread_t structure and starts
 * the passed function.  The arg argument is passed to the function.
 *
 * TODO - use the stacksize!
 */

int thread_create(thread_p *t, LPTHREAD_START_ROUTINE func, int stacksize, void *arg)
{
    pdebug(DEBUG_DETAIL, "Starting.");

    if(!t) {
        pdebug(DEBUG_WARN, "null pointer to thread pointer!");
        return PLCTAG_ERR_NULL_PTR;
    }

    *t = (thread_p)mem_alloc(sizeof(struct thread_t));
    if(! *t) {
        pdebug(DEBUG_WARN, "Unable to create new thread struct!");
        return PLCTAG_ERR_NO_MEM;
    }

    /* create a thread. */
    (*t)->h_thread = CreateThread(
                         NULL,                   /* default security attributes */
                         0,                      /* use default stack size      */
                         func,                   /* thread function             */
                         arg,                    /* argument to thread function */
                         0,                      /* use default creation flags  */
                         NULL);                  /* do not need thread ID       */

    if(!(*t)->h_thread) {
        pdebug(DEBUG_WARN, "error creating thread.");
        mem_free(*t);
        *t=NULL;

        return PLCTAG_ERR_THREAD_CREATE;
    }

    /* mark as initialized */
    (*t)->initialized = 1;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}




/*
 * thread_stop()
 *
 * Stop the current thread.  Does not take arguments.  Note: the thread
 * ends completely and this function does not return!
 */
void thread_stop(void)
{
    ExitThread((DWORD)0);
}


/*
 * thread_kill()
 *
 * Stop the indicated thread completely.
 */

void thread_kill(thread_p t)
{
    if(t) {
        TerminateThread(t->h_thread, (DWORD)0);
    }
}


/*
 * thread_join()
 *
 * Wait for the argument thread to stop and then continue.
 */

int thread_join(thread_p t)
{
    /*pdebug("Starting.");*/

    if(!t) {
        /*pdebug("null thread pointer.");*/
        return PLCTAG_ERR_NULL_PTR;
    }

    /* FIXME - check for uninitialized threads */

    if(WaitForSingleObject(t->h_thread, (DWORD)INFINITE)) {
        /*pdebug("Error joining thread.");*/
        return PLCTAG_ERR_THREAD_JOIN;
    }

    /*pdebug("Done.");*/

    return PLCTAG_STATUS_OK;
}




/*
 * thread_detach
 *
 * Detach the thread.  You cannot call thread_join on a detached thread!
 */

int thread_detach()
{
    /* FIXME - it does not look like you can do this on Windows??? */

    return PLCTAG_STATUS_OK;
}





/*
 * thread_destroy
 *
 * This gets rid of the resources of a thread struct.  The thread in
 * question must be dead first!
 */
int thread_destroy(thread_p *t)
{
    /*pdebug("Starting.");*/

    if(!t || ! *t) {
        /*pdebug("null thread pointer.");*/
        return PLCTAG_ERR_NULL_PTR;
    }

    CloseHandle((*t)->h_thread);

    mem_free(*t);

    *t = NULL;

    return PLCTAG_STATUS_OK;
}





/***************************************************************************
 ******************************* Atomic Ops ********************************
 **************************************************************************/

/*
 * lock_acquire
 *
 * Tries to write a non-zero value into the lock atomically.
 *
 * Returns non-zero on success.
 *
 * Warning: do not pass null pointers!
 */

#define ATOMIC_UNLOCK_VAL ((LONG)(0))
#define ATOMIC_LOCK_VAL ((LONG)(1))

int lock_acquire_try(lock_t *lock)
{
    LONG rc = InterlockedExchange(lock, ATOMIC_LOCK_VAL);

    if(rc != ATOMIC_LOCK_VAL) {
        return 1;
    } else {
        return 0;
    }
}


int lock_acquire(lock_t *lock)
{
    while(!lock_acquire_try(lock)) ;

    return 1;
}

void lock_release(lock_t *lock)
{
    InterlockedExchange(lock, ATOMIC_UNLOCK_VAL);
    /*pdebug("released lock");*/
}





/***************************************************************************
 ************************* Condition Variables *****************************
 ***************************************************************************/

struct cond_t {
    CRITICAL_SECTION cs;
    CONDITION_VARIABLE cond;
    int flag;
};

int cond_create(cond_p *c)
{
    int rc = PLCTAG_STATUS_OK;
    cond_p tmp_cond = NULL;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!c) {
        pdebug(DEBUG_WARN, "Null pointer to condition var pointer!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(*c) {
        pdebug(DEBUG_WARN, "Condition var pointer is not null, was it not deleted first?");
    }

    /* clear the output first. */
    *c = NULL;

    tmp_cond = (cond_p)mem_alloc((int)(unsigned int)sizeof(*tmp_cond));
    if(!tmp_cond) {
        pdebug(DEBUG_WARN, "Unable to allocate new condition var!");
        return PLCTAG_ERR_NO_MEM;
    }

    InitializeCriticalSection(&(tmp_cond->cs));
    InitializeConditionVariable(&(tmp_cond->cond));

    tmp_cond->flag = 0;

    *c = tmp_cond;

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}


int cond_wait_impl(const char* func, int line_num, cond_p c, int timeout_ms)
{
    int rc = PLCTAG_STATUS_OK;
    int64_t start_time = time_ms();

    pdebug(DEBUG_SPEW, "Starting. Called from %s:%d.", func, line_num);

    if (!c) {
        pdebug(DEBUG_WARN, "Condition var pointer is null in call from %s:%d!", func, line_num);
        return PLCTAG_ERR_NULL_PTR;
    }

    if (timeout_ms <= 0) {
        pdebug(DEBUG_WARN, "Timeout must be a positive value but was %d in call from %s:%d!", timeout_ms, func, line_num);
        return PLCTAG_ERR_BAD_PARAM;
    }


    EnterCriticalSection(&(c->cs));

    while(!c->flag) {
        int64_t time_left = (int64_t)timeout_ms - (time_ms() - start_time);

        if(time_left > 0) {
            int wait_rc = 0;

            if(SleepConditionVariableCS(&(c->cond), &(c->cs), (DWORD)time_left)) {
                /* we might need to wait again. could be a spurious wake up. */
                pdebug(DEBUG_SPEW, "Condition var wait returned.");
                rc = PLCTAG_STATUS_OK;
            } else {
                /* error or timeout. */
                wait_rc = GetLastError();
                if(wait_rc == ERROR_TIMEOUT) {
                    pdebug(DEBUG_SPEW, "Timeout response from condition var wait.");
                    rc = PLCTAG_ERR_TIMEOUT;
                    break;
                } else {
                    pdebug(DEBUG_WARN, "Error %d waiting on condition variable!", wait_rc);
                    rc = PLCTAG_ERR_BAD_STATUS;
                    break;
                }
            }
        } else {
            pdebug(DEBUG_SPEW, "Timed out.");
            rc = PLCTAG_ERR_TIMEOUT;
            break;
        }
    }

    if (c->flag) {
        pdebug(DEBUG_SPEW, "Condition var signaled for call at %s:%d.", func, line_num);

        /* clear the flag now that we've responded. */
        c->flag = 0;
    }
    else {
        pdebug(DEBUG_SPEW, "Condition wait terminated due to error or timeout for call at %s:%d.", func, line_num);
    }

    LeaveCriticalSection (&(c->cs));

    pdebug(DEBUG_SPEW, "Done for call at %s:%d.", func, line_num);

    return rc;
}


int cond_signal_impl(const char* func, int line_num, cond_p c)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW, "Starting.  Called from %s:%d.", func, line_num);

    if (!c) {
        pdebug(DEBUG_WARN, "Condition var pointer is null in call at %s:%d!", func, line_num);
        return PLCTAG_ERR_NULL_PTR;
    }

    EnterCriticalSection(&(c->cs));

    c->flag = 1;

    LeaveCriticalSection(&(c->cs));

    /* Windows does this outside the critical section? */
    WakeConditionVariable(&(c->cond));

    pdebug(DEBUG_SPEW, "Done for call at %s:%d.", func, line_num);

    return rc;
}



int cond_clear_impl(const char* func, int line_num, cond_p c)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW, "Starting.  Called from %s:%d.", func, line_num);

    if (!c) {
        pdebug(DEBUG_WARN, "Condition var pointer is null in call at %s:%d!", func, line_num);
        return PLCTAG_ERR_NULL_PTR;
    }

    EnterCriticalSection(&(c->cs));

    c->flag = 0;

    LeaveCriticalSection(&(c->cs));

    pdebug(DEBUG_SPEW, "Done for call at %s:%d.", func, line_num);

    return rc;
}


int cond_destroy(cond_p *c)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!c || ! *c) {
        pdebug(DEBUG_WARN, "Condition var pointer is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    mem_free(*c);

    *c = NULL;

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}







/***************************************************************************
 ******************************** Sockets **********************************
 **************************************************************************/


struct sock_t {
    SOCKET fd;
    SOCKET wake_read_fd;
    SOCKET wake_write_fd;
    int port;
    int is_open;
};


#define MAX_IPS (8)

static int sock_create_event_wakeup_channel(sock_p sock);


/*
 * Windows needs to have the Winsock library initialized
 * before use. Does it need to be static?
 *
 * Also set the timer period to handle the newer Windows 10 case
 * where it gets set fairly large (>15ms).
 */

static WSADATA wsaData = { 0 };

static int socket_lib_init(void)
{
    MMRESULT rc = 0;

    /*
    rc = timeBeginPeriod(WINDOWS_REQUESTED_TIMER_PERIOD_MS);
    if(rc != TIMERR_NOERROR) {
        pdebug(DEBUG_WARN, "Unable to set timer period to %ums!", WINDOWS_REQUESTED_TIMER_PERIOD_MS);
    }
    */

    return WSAStartup(MAKEWORD(2,2), &wsaData) == NO_ERROR;
}



int socket_create(sock_p *s)
{
    pdebug(DEBUG_DETAIL, "Starting.");

    if(!socket_lib_init()) {
        pdebug(DEBUG_WARN,"error initializing Windows Sockets.");
        return PLCTAG_ERR_WINSOCK;
    }

    if(!s) {
        pdebug(DEBUG_WARN, "null socket pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    *s = (sock_p)mem_alloc(sizeof(struct sock_t));

    if(! *s) {
        pdebug(DEBUG_ERROR, "Unable to allocate memory for socket!");
        return PLCTAG_ERR_NO_MEM;
    }

    (*s)->fd = INVALID_SOCKET;
    (*s)->wake_read_fd = INVALID_SOCKET;
    (*s)->wake_write_fd = INVALID_SOCKET;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}



int socket_connect_tcp_start(sock_p s, const char *host, int port)
{
    int rc = PLCTAG_STATUS_OK;
    IN_ADDR ips[MAX_IPS];
    int num_ips = 0;
    struct sockaddr_in gw_addr;
    int sock_opt = 1;
    u_long non_blocking=1;
    int i = 0;
    int done = 0;
    SOCKET fd;
    struct timeval timeout; /* used for timing out connections etc. */
    struct linger so_linger;

    pdebug(DEBUG_DETAIL, "Starting.");

    /* Open a socket for communication with the gateway. */
    fd = socket(AF_INET, SOCK_STREAM, 0/*IPPROTO_TCP*/);

    /* check for errors */
    if(fd < 0) {
        /*pdebug("Socket creation failed, errno: %d",errno);*/
        return PLCTAG_ERR_OPEN;
    }

    /* set up our socket to allow reuse if we crash suddenly. */
    sock_opt = 1;

    if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,(char*)&sock_opt, (int)sizeof(sock_opt))) {
        closesocket(fd);
        pdebug(DEBUG_WARN,"Error setting socket reuse option, errno: %d",errno);
        return PLCTAG_ERR_OPEN;
    }

    timeout.tv_sec = 10;
    timeout.tv_usec = 0;

    if(setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, (int)sizeof(timeout))) {
        closesocket(fd);
        pdebug(DEBUG_WARN,"Error setting socket receive timeout option, errno: %d",errno);
        return PLCTAG_ERR_OPEN;
    }

    if(setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char*)&timeout, (int)sizeof(timeout))) {
        closesocket(fd);
        pdebug(DEBUG_WARN,"Error setting socket send timeout option, errno: %d",errno);
        return PLCTAG_ERR_OPEN;
    }

    /* abort the connection on close. */
    so_linger.l_onoff = 1;
    so_linger.l_linger = 0;

    if(setsockopt(fd, SOL_SOCKET, SO_LINGER,(char*)&so_linger, (int)sizeof(so_linger))) {
        closesocket(fd);
        pdebug(DEBUG_ERROR,"Error setting socket close linger option, errno: %d",errno);
        return PLCTAG_ERR_OPEN;
    }

    /* figure out what address we are connecting to. */

    /* try a numeric IP address conversion first. */
    if(inet_pton(AF_INET,host,(struct in_addr *)ips) > 0) {
        pdebug(DEBUG_DETAIL, "Found numeric IP address: %s", host);
        num_ips = 1;
    } else {
        struct addrinfo hints;
        struct addrinfo* res_head = NULL;
        struct addrinfo *res = NULL;

        mem_set(&ips, 0, sizeof(ips));
        mem_set(&hints, 0, sizeof(hints));

        hints.ai_socktype = SOCK_STREAM; /* TCP */
        hints.ai_family = AF_INET; /* IP V4 only */

        if ((rc = getaddrinfo(host, NULL, &hints, &res_head)) != 0) {
            pdebug(DEBUG_WARN, "Error looking up PLC IP address %s, error = %d\n", host, rc);

            if (res_head) {
                freeaddrinfo(res_head);
            }

            closesocket(fd);
            return PLCTAG_ERR_BAD_GATEWAY;
        }

        res = res_head;
        for (num_ips = 0; res && num_ips < MAX_IPS; num_ips++) {
            ips[num_ips].s_addr = ((struct sockaddr_in *)(res->ai_addr))->sin_addr.s_addr;
            res = res->ai_next;
        }

        freeaddrinfo(res_head);
    }

    /* set the socket to non-blocking. */
    if (ioctlsocket(fd, FIONBIO, &non_blocking)) {
        /*pdebug("Error getting socket options, errno: %d", errno);*/
        closesocket(fd);
        return PLCTAG_ERR_OPEN;
    }

    pdebug(DEBUG_DETAIL, "Setting up wake pipe.");
    rc = sock_create_event_wakeup_channel(s);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to create wake channel, error %s!", plc_tag_decode_error(rc));
        return rc;
    }

    /*
     * now try to connect to the remote gateway.  We may need to
     * try several of the IPs we have.
     */

    i = 0;
    done = 0;

    memset((void *)&gw_addr,0, sizeof(gw_addr));
    gw_addr.sin_family = AF_INET ;
    gw_addr.sin_port = htons(port);

    do {
        /* try each IP until we run out or get a connection. */
        gw_addr.sin_addr.s_addr = ips[i].s_addr;

        /*pdebug(DEBUG_DETAIL,"Attempting to connect to %s",inet_ntoa(*((struct in_addr *)&ips[i])));*/

        rc = connect(fd,(struct sockaddr *)&gw_addr,sizeof(gw_addr));

        /* connect returns SOCKET_ERROR and a code of WSAEWOULDBLOCK on non-blocking sockets. */
        if(rc == SOCKET_ERROR) {
            int sock_err = WSAGetLastError();
            if (sock_err == WSAEWOULDBLOCK) {
                pdebug(DEBUG_DETAIL, "Socket connection attempt %d started successfully.", i);
                rc = PLCTAG_STATUS_PENDING;
                done = 1;
            } else {
                pdebug(DEBUG_WARN, "Error %d trying to start connection attempt %d process!  Trying next IP address.", sock_err, i);
                i++;
            }
        } else {
            pdebug(DEBUG_DETAIL, "Socket connection attempt %d succeeded immediately.", i);
            rc = PLCTAG_STATUS_OK;
            done = 1;
        }
    } while(!done && i < num_ips);

    if(!done) {
        closesocket(fd);
        pdebug(DEBUG_WARN,"Unable to connect to any gateway host IP address!");
        return PLCTAG_ERR_OPEN;
    }

    /* save the values */
    s->fd = fd;
    s->port = port;

    s->is_open = 1;

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}





int socket_connect_tcp_check(sock_p sock, int timeout_ms)
{
    int rc = PLCTAG_STATUS_OK;
    fd_set write_set;
    fd_set err_set;
    struct timeval tv;
    int select_rc = 0;

    pdebug(DEBUG_DETAIL, "Starting.");

    if (!sock) {
        pdebug(DEBUG_WARN, "Null socket pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* wait for the socket to be ready. */
    tv.tv_sec = (long)(timeout_ms / 1000);
    tv.tv_usec = (long)(timeout_ms % 1000) * (long)(1000);

    /* Windows reports connection errors on the exception/error socket set. */
    FD_ZERO(&write_set);
    FD_SET(sock->fd, &write_set);
    FD_ZERO(&err_set);
    FD_SET(sock->fd, &err_set);

    select_rc = select((int)(sock->fd) + 1, NULL, &write_set, &err_set, &tv);
    if(select_rc == 1) {
        if(FD_ISSET(sock->fd, &write_set)) {
            pdebug(DEBUG_DETAIL, "Socket is connected.");
            rc = PLCTAG_STATUS_OK;
        } else if(FD_ISSET(sock->fd, &err_set)) {
            pdebug(DEBUG_WARN, "Error connecting!");
            return PLCTAG_ERR_OPEN;
        } else {
            pdebug(DEBUG_WARN, "select() returned a 1, but no sockets are selected!");
            return PLCTAG_ERR_OPEN;
        }
    } else if(select_rc == 0) {
        pdebug(DEBUG_DETAIL, "Socket connection not done yet.");
        rc = PLCTAG_ERR_TIMEOUT;
    } else {
        int err = WSAGetLastError();

        pdebug(DEBUG_WARN, "select() has error %d!", err);

        switch (err) {
        case WSAENETDOWN: /* The network subsystem is down */
            pdebug(DEBUG_WARN, "The network subsystem is down!");
            return PLCTAG_ERR_OPEN;
            break;

        case WSANOTINITIALISED: /*Winsock was not initialized. */
            pdebug(DEBUG_WARN, "WSAStartup() was not called to initialize the Winsock subsystem.!");
            return PLCTAG_ERR_OPEN;
            break;

        case WSAEINVAL: /* The arguments to select() were bad. */
            pdebug(DEBUG_WARN, "One or more of the arguments to select() were invalid!");
            return PLCTAG_ERR_OPEN;
            break;

        case WSAEFAULT: /* No mem/resources for select. */
            pdebug(DEBUG_WARN, "Insufficient memory or resources for select() to run!");
            return PLCTAG_ERR_NO_MEM;
            break;

        case WSAEINTR: /* A blocking Windows Socket 1.1 call was canceled through WSACancelBlockingCall.  */
            pdebug(DEBUG_WARN, "A blocking Winsock call was canceled!");
            return PLCTAG_ERR_OPEN;
            break;

        case WSAEINPROGRESS: /* A blocking Windows Socket 1.1 call is in progress.  */
            pdebug(DEBUG_WARN, "A blocking Winsock call is in progress!");
            return PLCTAG_ERR_OPEN;
            break;

        case WSAENOTSOCK: /* One or more of the FDs in the set is not a socket. */
            pdebug(DEBUG_WARN, "The fd in the FD set is not a socket!");
            return PLCTAG_ERR_OPEN;
            break;

        default:
            pdebug(DEBUG_WARN, "Unexpected err %d from select()!", err);
            return PLCTAG_ERR_OPEN;
            break;
        }
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}


int socket_wait_event(sock_p sock, int events, int timeout_ms)
{
    int result = SOCK_EVENT_NONE;
    fd_set read_set;
    fd_set write_set;
    fd_set err_set;
    int num_sockets = 0;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!sock) {
        pdebug(DEBUG_WARN, "Null socket pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!sock->is_open) {
        pdebug(DEBUG_WARN, "Socket is not open!");
        return PLCTAG_ERR_READ;
    }

    if(timeout_ms < 0) {
        pdebug(DEBUG_WARN, "Timeout must be zero or positive!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* check if the mask is empty */
    if(events == 0) {
        pdebug(DEBUG_WARN, "Passed event mask is empty!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* set up fd sets */
    FD_ZERO(&read_set);
    FD_ZERO(&write_set);
    FD_ZERO(&err_set);

    /* add the wake fd */
    FD_SET(sock->wake_read_fd, &read_set);

    /* we always want to know about errors. */
    FD_SET(sock->fd, &err_set);

    /* add more depending on the mask. */
    if(events & SOCK_EVENT_CAN_READ) {
        FD_SET(sock->fd, &read_set);
    }

    if((events & SOCK_EVENT_CONNECT) || (events & SOCK_EVENT_CAN_WRITE)) {
        FD_SET(sock->fd, &write_set);
    }

    /* calculate the timeout. */
    if(timeout_ms > 0) {
        struct timeval tv;

        tv.tv_sec = (long)(timeout_ms / 1000);
        tv.tv_usec = (long)(timeout_ms % 1000) * (long)(1000);

        num_sockets = select(2, &read_set, &write_set, &err_set, &tv);
    } else {
        num_sockets = select(2, &read_set, &write_set, &err_set, NULL);
    }

    if(num_sockets == 0) {
        result |= (events & SOCK_EVENT_TIMEOUT);
    } else if(num_sockets > 0) {
        /* was there a wake up? */
        if(FD_ISSET(sock->wake_read_fd, &read_set)) {
            int bytes_read = 0;
            char buf[32];

            /* empty the socket. */
            while((bytes_read = (int)recv(sock->wake_read_fd, (char*)&buf[0], sizeof(buf), 0)) > 0) { }

            pdebug(DEBUG_DETAIL, "Socket woken up.");

            result |= (events & SOCK_EVENT_WAKE_UP);
        }

        /* is read ready for the main fd? */
        if(FD_ISSET(sock->fd, &read_set)) {
            char buf;
            int byte_read = 0;

            byte_read = (int)recv(sock->fd, &buf, sizeof(buf), MSG_PEEK);

            if(byte_read) {
                pdebug(DEBUG_DETAIL, "Socket can read.");
                result |= (events & SOCK_EVENT_CAN_READ);
            } else {
                pdebug(DEBUG_DETAIL, "Socket disconnected.");
                result |= (events & SOCK_EVENT_DISCONNECT);
            }
        }

        /* is write ready for the main fd? */
        if(FD_ISSET(sock->fd, &write_set)) {
            pdebug(DEBUG_DETAIL, "Socket can write or just connected.");
            result |= ((events & SOCK_EVENT_CAN_WRITE) | (events & SOCK_EVENT_CONNECT));
        }

        /* is there an error? */
        if(FD_ISSET(sock->fd, &err_set)) {
            pdebug(DEBUG_DETAIL, "Socket has error!");
            result |= (events & SOCK_EVENT_ERROR);
        }
    } else {
        int err = WSAGetLastError();

        pdebug(DEBUG_WARN, "select() returned status %d!", num_sockets);

        switch(err) {
            case WSANOTINITIALISED: /* WSAStartup() not called first. */
                pdebug(DEBUG_WARN, "WSAStartUp() not called before calling Winsock functions!");
                return PLCTAG_ERR_BAD_CONFIG;
                break;

            case WSAEFAULT: /* No mem for internal tables. */
                pdebug(DEBUG_WARN, "Insufficient resources for select() to run!");
                return PLCTAG_ERR_NO_MEM;
                break;

            case WSAENETDOWN: /* network subsystem is down. */
                pdebug(DEBUG_WARN, "The network subsystem is down!");
                return PLCTAG_ERR_BAD_DEVICE;
                break;

            case WSAEINVAL: /* timeout is invalid. */
                pdebug(DEBUG_WARN, "The timeout is invalid!");
                return PLCTAG_ERR_BAD_PARAM;
                break;

            case WSAEINTR: /* A blocking call wss cancelled. */
                pdebug(DEBUG_WARN, "A blocking call was cancelled!");
                return PLCTAG_ERR_BAD_CONFIG;
                break;

            case WSAEINPROGRESS: /* A blocking call is already in progress. */
                pdebug(DEBUG_WARN, "A blocking call is already in progress!");
                return PLCTAG_ERR_BAD_CONFIG;
                break;

            case WSAENOTSOCK: /* The descriptor set contains something other than a socket. */
                pdebug(DEBUG_WARN, "The fd set contains something other than a socket!");
                return PLCTAG_ERR_BAD_DATA;
                break;

            default:
                pdebug(DEBUG_WARN, "Unexpected socket err %d!", err);
                return PLCTAG_ERR_BAD_STATUS;
                break;
        }
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return result;
}



int socket_wake(sock_p sock)
{
    int rc = PLCTAG_STATUS_OK;
    const char dummy_data[] = "Dummy data.";

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!sock) {
        pdebug(DEBUG_WARN, "Null socket pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!sock->is_open) {
        pdebug(DEBUG_WARN, "Socket is not open!");
        return PLCTAG_ERR_READ;
    }

    rc = send(sock->wake_write_fd, (const char *)dummy_data, sizeof(dummy_data), (int)MSG_NOSIGNAL);
    if(rc < 0) {
        int err = WSAGetLastError();

        if(err == WSAEWOULDBLOCK) {
            pdebug(DEBUG_DETAIL, "Write wrote no data.");

            rc = PLCTAG_STATUS_OK;
        } else {
            pdebug(DEBUG_WARN,"socket write error rc=%d, errno=%d", rc, err);
            return PLCTAG_ERR_WRITE;
        }
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}



int socket_read(sock_p s, uint8_t *buf, int size, int timeout_ms)
{
    int rc;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!s) {
        pdebug(DEBUG_WARN, "Socket pointer is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!buf) {
        pdebug(DEBUG_WARN, "Buffer pointer is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!s->is_open) {
        pdebug(DEBUG_WARN, "Socket is not open!");
        return PLCTAG_ERR_READ;
    }

    if(timeout_ms < 0) {
        pdebug(DEBUG_WARN, "Timeout must be zero or positive!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* try to read without waiting.   Saves a system call if it works. */
    rc = recv(s->fd, (char *)buf, size, 0);
    if(rc < 0) {
        int err = WSAGetLastError();

        if(err == WSAEWOULDBLOCK) {
            if(timeout_ms > 0) {
                pdebug(DEBUG_DETAIL, "Immediate read attempt did not succeed, now wait for select().");
            } else {
                pdebug(DEBUG_DETAIL, "Read resulted in no data.");
            }

            rc = 0;
        } else {
            pdebug(DEBUG_WARN,"socket read error rc=%d, errno=%d", rc, err);
            return PLCTAG_ERR_READ;
        }
    }

    /* only wait if we have a timeout and no data and no error. */
    if(rc == 0 && timeout_ms > 0) {
        fd_set read_set;
        TIMEVAL tv;
        int select_rc = 0;

        tv.tv_sec = (long)(timeout_ms / 1000);
        tv.tv_usec = (long)(timeout_ms % 1000) * (long)(1000);

        FD_ZERO(&read_set);

        FD_SET(s->fd, &read_set);

        select_rc = select(1, &read_set, NULL, NULL, &tv);
        if(select_rc == 1) {
            if(FD_ISSET(s->fd, &read_set)) {
                pdebug(DEBUG_DETAIL, "Socket can read data.");
            } else {
                pdebug(DEBUG_WARN, "select() returned but socket is not ready to read data!");
                return PLCTAG_ERR_BAD_REPLY;
            }
        } else if(select_rc == 0) {
            pdebug(DEBUG_DETAIL, "Socket read timed out.");
            return PLCTAG_ERR_TIMEOUT;
        } else {
            int err = WSAGetLastError();

            pdebug(DEBUG_WARN, "select() returned status %d!", select_rc);

            switch(err) {
                case WSANOTINITIALISED: /* WSAStartup() not called first. */
                    pdebug(DEBUG_WARN, "WSAStartUp() not called before calling Winsock functions!");
                    return PLCTAG_ERR_BAD_CONFIG;
                    break;

                case WSAEFAULT: /* No mem for internal tables. */
                    pdebug(DEBUG_WARN, "Insufficient resources for select() to run!");
                    return PLCTAG_ERR_NO_MEM;
                    break;

                case WSAENETDOWN: /* network subsystem is down. */
                    pdebug(DEBUG_WARN, "The network subsystem is down!");
                    return PLCTAG_ERR_BAD_DEVICE;
                    break;

                case WSAEINVAL: /* timeout is invalid. */
                    pdebug(DEBUG_WARN, "The timeout is invalid!");
                    return PLCTAG_ERR_BAD_PARAM;
                    break;

                case WSAEINTR: /* A blocking call wss cancelled. */
                    pdebug(DEBUG_WARN, "A blocking call was cancelled!");
                    return PLCTAG_ERR_BAD_CONFIG;
                    break;

                case WSAEINPROGRESS: /* A blocking call is already in progress. */
                    pdebug(DEBUG_WARN, "A blocking call is already in progress!");
                    return PLCTAG_ERR_BAD_CONFIG;
                    break;

                case WSAENOTSOCK: /* The descriptor set contains something other than a socket. */
                    pdebug(DEBUG_WARN, "The fd set contains something other than a socket!");
                    return PLCTAG_ERR_BAD_DATA;
                    break;

                default:
                    pdebug(DEBUG_WARN, "Unexpected socket err %d!", err);
                    return PLCTAG_ERR_BAD_STATUS;
                    break;
            }
        }

        /* select() returned saying we can read, so read. */
        rc = recv(s->fd, (char *)buf, size, 0);
        if(rc < 0) {
            int err = WSAGetLastError();

            if(err == WSAEWOULDBLOCK) {
                rc = 0;
            } else {
                pdebug(DEBUG_WARN,"socket read error rc=%d, errno=%d", rc, err);
                return PLCTAG_ERR_READ;
            }
        }
    }

    pdebug(DEBUG_DETAIL, "Done: result = %d.", rc);

    return rc;
}



int socket_write(sock_p s, uint8_t *buf, int size, int timeout_ms)
{
    int rc;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!s) {
        pdebug(DEBUG_WARN, "Socket pointer is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!buf) {
        pdebug(DEBUG_WARN, "Buffer pointer is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!s->is_open) {
        pdebug(DEBUG_WARN, "Socket is not open!");
        return PLCTAG_ERR_READ;
    }

    if(timeout_ms < 0) {
        pdebug(DEBUG_WARN, "Timeout must be zero or positive!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    rc = send(s->fd, (const char *)buf, size, (int)MSG_NOSIGNAL);
    if(rc < 0) {
        int err = WSAGetLastError();

        if(err == WSAEWOULDBLOCK) {
            if(timeout_ms > 0) {
                pdebug(DEBUG_DETAIL, "Immediate write attempt did not succeed, now wait for select().");
            } else {
                pdebug(DEBUG_DETAIL, "Write wrote no data.");
            }

            rc = 0;
        } else {
            pdebug(DEBUG_WARN,"socket write error rc=%d, errno=%d", rc, err);
            return PLCTAG_ERR_WRITE;
        }
    }

    /* only wait if we have a timeout and no data. */
    if(rc == 0 && timeout_ms > 0) {
        fd_set write_set;
        TIMEVAL tv;
        int select_rc = 0;

        tv.tv_sec = (long)(timeout_ms / 1000);
        tv.tv_usec = (long)(timeout_ms % 1000) * (long)(1000);

        FD_ZERO(&write_set);

        FD_SET(s->fd, &write_set);

        select_rc = select(1, NULL, &write_set, NULL, &tv);
        if(select_rc == 1) {
            if(FD_ISSET(s->fd, &write_set)) {
                pdebug(DEBUG_DETAIL, "Socket can write data.");
            } else {
                pdebug(DEBUG_WARN, "select() returned but socket is not ready to write data!");
                return PLCTAG_ERR_BAD_REPLY;
            }
        } else if(select_rc == 0) {
            pdebug(DEBUG_DETAIL, "Socket write timed out.");
            return PLCTAG_ERR_TIMEOUT;
        } else {
            int err = WSAGetLastError();

            pdebug(DEBUG_WARN, "select() returned status %d!", select_rc);

            switch(err) {
                case WSANOTINITIALISED: /* WSAStartup() not called first. */
                    pdebug(DEBUG_WARN, "WSAStartUp() not called before calling Winsock functions!");
                    return PLCTAG_ERR_BAD_CONFIG;
                    break;

                case WSAEFAULT: /* No mem for internal tables. */
                    pdebug(DEBUG_WARN, "Insufficient resources for select() to run!");
                    return PLCTAG_ERR_NO_MEM;
                    break;

                case WSAENETDOWN: /* network subsystem is down. */
                    pdebug(DEBUG_WARN, "The network subsystem is down!");
                    return PLCTAG_ERR_BAD_DEVICE;
                    break;

                case WSAEINVAL: /* timeout is invalid. */
                    pdebug(DEBUG_WARN, "The timeout is invalid!");
                    return PLCTAG_ERR_BAD_PARAM;
                    break;

                case WSAEINTR: /* A blocking call wss cancelled. */
                    pdebug(DEBUG_WARN, "A blocking call was cancelled!");
                    return PLCTAG_ERR_BAD_CONFIG;
                    break;

                case WSAEINPROGRESS: /* A blocking call is already in progress. */
                    pdebug(DEBUG_WARN, "A blocking call is already in progress!");
                    return PLCTAG_ERR_BAD_CONFIG;
                    break;

                case WSAENOTSOCK: /* The descriptor set contains something other than a socket. */
                    pdebug(DEBUG_WARN, "The fd set contains something other than a socket!");
                    return PLCTAG_ERR_BAD_DATA;
                    break;

                default:
                    pdebug(DEBUG_WARN, "Unexpected socket err %d!", err);
                    return PLCTAG_ERR_BAD_STATUS;
                    break;
            }
        }

        /* try to write since select() said we could. */
        rc = send(s->fd, (const char *)buf, size, (int)MSG_NOSIGNAL);
        if(rc < 0) {
            int err = WSAGetLastError();

            if(err == WSAEWOULDBLOCK) {
                pdebug(DEBUG_DETAIL, "No data written.");
                rc = 0;
            } else {
                pdebug(DEBUG_WARN,"socket write error rc=%d, errno=%d", rc, err);
                return PLCTAG_ERR_WRITE;
            }
        }
    }

    pdebug(DEBUG_DETAIL, "Done: result = %d.", rc);

    return rc;
}



int socket_close(sock_p s)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    if(!s) {
        pdebug(DEBUG_WARN, "Socket pointer or pointer to socket pointer is NULL!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(s->wake_read_fd != INVALID_SOCKET) {
        if(closesocket(s->wake_read_fd)) {
            pdebug(DEBUG_WARN, "Error closing wake read socket!");
            rc = PLCTAG_ERR_CLOSE;
        }

        s->wake_read_fd = INVALID_SOCKET;
    }

    if(s->wake_write_fd != INVALID_SOCKET) {
        if(closesocket(s->wake_write_fd)) {
            pdebug(DEBUG_WARN, "Error closing wake write socket!");
            rc = PLCTAG_ERR_CLOSE;
        }

        s->wake_write_fd = INVALID_SOCKET;
    }

    if(s->fd != INVALID_SOCKET) {
        if(closesocket(s->fd)) {
            pdebug(DEBUG_WARN, "Error closing socket!");
            rc = PLCTAG_ERR_CLOSE;
        }

        s->fd = INVALID_SOCKET;
    }

    s->is_open = 0;

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}



int socket_destroy(sock_p *s)
{
    pdebug(DEBUG_INFO, "Starting.");

    if(!s || !*s) {
        pdebug(DEBUG_WARN, "Socket pointer or pointer to socket pointer is NULL!");
        return PLCTAG_ERR_NULL_PTR;
    }

    socket_close(*s);

    mem_free(*s);

    *s = 0;

    if(WSACleanup() != NO_ERROR) {
        return PLCTAG_ERR_WINSOCK;
    }

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_OK;
}



int sock_create_event_wakeup_channel(sock_p sock)
{
    int rc = PLCTAG_STATUS_OK;
    SOCKET listener = INVALID_SOCKET;
    struct sockaddr_in listener_addr_info;
    socklen_t addr_info_size = sizeof(struct sockaddr_in);
    u_long non_blocking = 1;
    SOCKET wake_fds[2];

    pdebug(DEBUG_INFO, "Starting.");

    wake_fds[0] = INVALID_SOCKET;
    wake_fds[1] = INVALID_SOCKET;

    /*
     * This is a bit convoluted.
     *
     * First we open a listening socket on the loopback interface.
     * We do not care what port so we let the OS decide.
     *
     * Then we connect to that socket.   The connection becomes
     * the reader side of the wake up fds.
     *
     * Then we accept and that becomes the writer side of the
     * wake up fds.
     *
     * Then we close the listener because we do not want to keep
     * it open as it might be a problem.  Probably more for DOS
     * purposes than any security, but you never know!
     *
     * And the reader and writer have to be set up as non-blocking!
     *
     * This was cobbled together from various sources including
     * StackExchange and MSDN.   I did not take notes, so I am unable
     * to properly credit the original sources :-(
     */

    do {
        /*
         * Set up our listening socket.
         */

        listener = (SOCKET)socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if(listener < 0) {
            pdebug(DEBUG_WARN, "Error %d creating the listener socket!", WSAGetLastError());
            rc = PLCTAG_ERR_WINSOCK;
            break;
        }

        /* clear the listener address info */
        mem_set(&listener_addr_info, 0, addr_info_size);

        /* standard IPv4 for the win! */
        listener_addr_info.sin_family = AF_INET;

        /* we do not care what port. */
        listener_addr_info.sin_port = 0;

        /* we want to connect on the loopback address. */
        listener_addr_info.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

        /* now comes the part where we could fail. */

        /* first we bind the listener to the loopback and let the OS choose the port. */
        if(bind(listener, (struct sockaddr *)&listener_addr_info, addr_info_size)){
            pdebug(DEBUG_WARN, "Error %d binding the listener socket!", WSAGetLastError());
            rc = PLCTAG_ERR_WINSOCK;
            break;
        }

        /*
         * we need to get the address and port of the listener for later steps.
         * Notice that this _sets_ the address size!.
         */
        if(getsockname(listener, (struct sockaddr *)&listener_addr_info, &addr_info_size)) {
            pdebug(DEBUG_WARN, "Error %d getting the listener socket address info!", WSAGetLastError());
            rc = PLCTAG_ERR_WINSOCK;
            break;
        }

        /* Phwew.   We can actually listen now. Notice that this is blocking! */
        if(listen(listener, 1)) { /* MAGIC constant - We do not want any real queue! */
            pdebug(DEBUG_WARN, "Error %d listening on the listener socket!", WSAGetLastError());
            rc = PLCTAG_ERR_WINSOCK;
            break;
        }

        /*
         * Set up our wake read side socket.
         */

        wake_fds[0] = (SOCKET)socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (wake_fds[0] < 0) {
            pdebug(DEBUG_WARN, "Error %d creating the wake channel read side socket!", WSAGetLastError());
            rc = PLCTAG_ERR_WINSOCK;
            break;
        }

        /*
         * now we start the next phase.   We need to connect to our own listener.
         * This will be the reader side of the wake up socket.
         */

        if(connect(wake_fds[0], (struct sockaddr *)&listener_addr_info, addr_info_size)) {
            pdebug(DEBUG_WARN, "Error %d connecting to the listener socket!", WSAGetLastError());
            rc = PLCTAG_ERR_WINSOCK;
            break;
        }

        /* now we accept our own connection. This becomes the writer side. */
        wake_fds[1] = accept(listener, 0, 0);
        if (wake_fds[1] == INVALID_SOCKET) {
            pdebug(DEBUG_WARN, "Error %d connecting to the listener socket!", WSAGetLastError());
            rc = PLCTAG_ERR_WINSOCK;
            break;
        }

        /* now we need to set these to non-blocking. */

        /* reader */
        if(ioctlsocket(wake_fds[0], FIONBIO, &non_blocking)) {
            pdebug(DEBUG_WARN, "Error %d setting reader socket to non-blocking!", WSAGetLastError());
            rc = PLCTAG_ERR_WINSOCK;
            break;
        }

        /* writer */
        if(ioctlsocket(wake_fds[1], FIONBIO, &non_blocking)) {
            pdebug(DEBUG_WARN, "Error %d setting reader socket to non-blocking!", WSAGetLastError());
            rc = PLCTAG_ERR_WINSOCK;
            break;
        }
    } while(0);

    /* do some clean up */
    if(listener != INVALID_SOCKET) {
        closesocket(listener);
    }

    /* check the result */
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to set up wakeup socket!");

        if(wake_fds[0] != INVALID_SOCKET) {
            closesocket(wake_fds[0]);
            wake_fds[0] = INVALID_SOCKET;
        }

        if(wake_fds[1] != INVALID_SOCKET) {
            closesocket(wake_fds[1]);
            wake_fds[1] = INVALID_SOCKET;
        }
    } else {
        sock->wake_read_fd = wake_fds[0];
        sock->wake_write_fd = wake_fds[1];

        pdebug(DEBUG_INFO, "Done.");
    }

    return rc;
}




/***************************************************************************
 ****************************** Serial Port ********************************
 **************************************************************************/


struct serial_port_t {
    HANDLE hSerialPort;
    COMMCONFIG oldDCBSerialParams;
    COMMTIMEOUTS oldTimeouts;
};


serial_port_p plc_lib_open_serial_port(/*plc_lib lib,*/ const char *path, int baud_rate, int data_bits, int stop_bits, int parity_type)
{
    serial_port_p serial_port;
    COMMCONFIG dcbSerialParams;
    COMMTIMEOUTS timeouts;
    HANDLE hSerialPort;
    int BAUD, PARITY, DATABITS, STOPBITS;

    //plc_err(lib, PLC_LOG_DEBUG, PLC_ERR_NONE, "Starting.");


    /* create the configuration for the serial port. */

    /* code largely from Programmer's Heaven.
     */

    switch (baud_rate) {
    case 38400:
        BAUD = CBR_38400;
        break;
    case 19200:
        BAUD  = CBR_19200;
        break;
    case 9600:
        BAUD  = CBR_9600;
        break;
    case 4800:
        BAUD  = CBR_4800;
        break;
    case 2400:
        BAUD  = CBR_2400;
        break;
    case 1200:
        BAUD  = CBR_1200;
        break;
    case 600:
        BAUD  = CBR_600;
        break;
    case 300:
        BAUD  = CBR_300;
        break;
    case 110:
        BAUD  = CBR_110;
        break;
    default:
        /* unsupported baud rate */
        //plc_err(lib, PLC_LOG_ERR, PLC_ERR_BAD_PARAM,"Unsupported baud rate: %d. Use standard baud rates (300,600,1200,2400...).",baud_rate);
        return NULL;
    }


    /* data bits */
    switch(data_bits) {
    case 5:
        DATABITS = 5;
        break;

    case 6:
        DATABITS = 6;
        break;

    case 7:
        DATABITS = 7;
        break;

    case 8:
        DATABITS = 8;
        break;

    default:
        /* unsupported number of data bits. */
        //plc_err(lib, PLC_LOG_ERR, PLC_ERR_BAD_PARAM,"Unsupported number of data bits: %d. Use 5-8.",data_bits);
        return NULL;
    }


    switch(stop_bits) {
    case 1:
        STOPBITS = ONESTOPBIT;
        break;
    case 2:
        STOPBITS = TWOSTOPBITS;
        break;
    default:
        /* unsupported number of stop bits. */
        //plc_err(lib, PLC_LOG_ERR, PLC_ERR_BAD_PARAM,"Unsupported number of stop bits, %d, must be 1 or 2.",stop_bits);
        return NULL;
    }

    switch(parity_type) {
    case 0:
        PARITY = NOPARITY;
        break;
    case 1: /* Odd parity */
        PARITY = ODDPARITY;
        break;
    case 2: /* Even parity */
        PARITY = EVENPARITY;
        break;
    default:
        /* unsupported number of stop bits. */
        //plc_err(lib, PLC_LOG_ERR, PLC_ERR_BAD_PARAM,"Unsupported parity type, must be none (0), odd (1) or even (2).");
        return NULL;
    }

    /* allocate the structure */
    serial_port = (serial_port_p)calloc(1,sizeof(struct serial_port_t));

    if(!serial_port) {
        //plc_err(lib, PLC_LOG_ERR, PLC_ERR_NO_MEM, "Unable to allocate serial port struct.");
        return NULL;
    }

    /* open the serial port device */
    hSerialPort = CreateFileA(path,
                             GENERIC_READ | GENERIC_WRITE,
                             0,
                             NULL,
                             OPEN_EXISTING,
                             FILE_ATTRIBUTE_NORMAL,
                             NULL);

    /* did the open succeed? */
    if(hSerialPort == INVALID_HANDLE_VALUE) {
        free(serial_port);
        //plc_err(lib, PLC_LOG_ERR, PLC_ERR_OPEN, "Error opening serial device %s",path);
        return NULL;
    }

    /* get existing serial port configuration and save it. */
    if(!GetCommState(hSerialPort, &(serial_port->oldDCBSerialParams.dcb))) {
        free(serial_port);
        CloseHandle(hSerialPort);
        //plc_err(lib, PLC_LOG_ERR, PLC_ERR_OPEN, "Error getting backup serial port configuration.",path);
        return NULL;
    }

    /* copy the params. */
    dcbSerialParams = serial_port->oldDCBSerialParams;

    dcbSerialParams.dcb.BaudRate = BAUD;
    dcbSerialParams.dcb.ByteSize = DATABITS;
    dcbSerialParams.dcb.StopBits = STOPBITS;
    dcbSerialParams.dcb.Parity = PARITY;

    dcbSerialParams.dcb.fBinary         = TRUE;
    dcbSerialParams.dcb.fDtrControl     = DTR_CONTROL_DISABLE;
    dcbSerialParams.dcb.fRtsControl     = RTS_CONTROL_DISABLE;
    dcbSerialParams.dcb.fOutxCtsFlow    = FALSE;
    dcbSerialParams.dcb.fOutxDsrFlow    = FALSE;
    dcbSerialParams.dcb.fDsrSensitivity = FALSE;
    dcbSerialParams.dcb.fAbortOnError   = TRUE; /* TODO - should this be false? */

    /* attempt to set the serial params */
    if(!SetCommState(hSerialPort, &dcbSerialParams.dcb)) {
        free(serial_port);
        CloseHandle(hSerialPort);
        //plc_err(lib, PLC_LOG_ERR, PLC_ERR_OPEN, "Error setting serial port configuration.",path);
        return NULL;
    }

    /* attempt to get the current serial port timeout set up */
    if(!GetCommTimeouts(hSerialPort, &(serial_port->oldTimeouts))) {
        SetCommState(hSerialPort, &(serial_port->oldDCBSerialParams.dcb));
        free(serial_port);
        CloseHandle(hSerialPort);
        //plc_err(lib, PLC_LOG_ERR, PLC_ERR_OPEN, "Error getting backup serial port timeouts.",path);
        return NULL;
    }

    timeouts = serial_port->oldTimeouts;

    /* set the timeouts for asynch operation */
    timeouts.ReadIntervalTimeout = MAXDWORD;
    timeouts.ReadTotalTimeoutMultiplier = 0;
    timeouts.ReadTotalTimeoutConstant = 0;
    timeouts.WriteTotalTimeoutMultiplier = 0;
    timeouts.WriteTotalTimeoutConstant = 0;

    /* attempt to set the current serial port timeout set up */
    if(!SetCommTimeouts(hSerialPort, &timeouts)) {
        SetCommState(hSerialPort, &(serial_port->oldDCBSerialParams.dcb));
        free(serial_port);
        CloseHandle(hSerialPort);
        //plc_err(lib, PLC_LOG_ERR, PLC_ERR_OPEN, "Error getting backup serial port timeouts.",path);
        return NULL;
    }

    return serial_port;
}







int plc_lib_close_serial_port(serial_port_p serial_port)
{
    /* try to prevent this from being called twice */
    if(!serial_port || !serial_port->hSerialPort) {
        return 1;
    }

    /* reset the old options */
    SetCommTimeouts(serial_port->hSerialPort, &(serial_port->oldTimeouts));
    SetCommState(serial_port->hSerialPort, &(serial_port->oldDCBSerialParams.dcb));
    CloseHandle(serial_port->hSerialPort);

    /* make sure that we do not call this twice. */
    serial_port->hSerialPort = 0;

    /* free the serial port */
    free(serial_port);

    return 1;
}




int plc_lib_serial_port_read(serial_port_p serial_port, uint8_t *data, int size)
{
    DWORD numBytesRead = 0;
    BOOL rc;

    rc = ReadFile(serial_port->hSerialPort,(LPVOID)data,(DWORD)size,&numBytesRead,NULL);

    if(rc != TRUE)
        return -1;

    return (int)numBytesRead;
}


int plc_lib_serial_port_write(serial_port_p serial_port, uint8_t *data, int size)
{
    DWORD numBytesWritten = 0;
    BOOL rc;

    rc = WriteFile(serial_port->hSerialPort,(LPVOID)data,(DWORD)size,&numBytesWritten,NULL);

    return (int)numBytesWritten;
}











/***************************************************************************
 ***************************** Miscellaneous *******************************
 **************************************************************************/






int sleep_ms(int ms)
{
    Sleep(ms);
    return 1;
}



/*
 * time_ms
 *
 * Return current system time in millisecond units.  This is NOT an
 * Unix epoch time.  Windows uses a different epoch starting 1/1/1601.
 */

int64_t time_ms(void)
{
    FILETIME ft;
    int64_t res;

    GetSystemTimeAsFileTime(&ft);

    /* calculate time as 100ns increments since Jan 1, 1601. */
    res = (int64_t)(ft.dwLowDateTime) + ((int64_t)(ft.dwHighDateTime) << 32);

    /* get time in ms.   Magic offset is for Jan 1, 1970 Unix epoch baseline. */
    res = (res - 116444736000000000) / 10000;

    return  res;
}


struct tm *localtime_r(const time_t *timep, struct tm *result)
{
    time_t t = *timep;

    localtime_s(result, &t);

    return result;
}

#else

#define _GNU_SOURCE

#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <limits.h>
#include <math.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <time.h>
#include <inttypes.h>


#if defined(__APPLE__) || defined(__FreeBSD__) ||  defined(__NetBSD__) || defined(__OpenBSD__) || defined(__bsdi__) || defined(__DragonFly__)
    #define BSD_OS_TYPE
    #if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
        #define _DARWIN_C_SOURCE _POSIX_C_SOURCE
    #endif
#endif


/***************************************************************************
 ******************************* Memory ************************************
 **************************************************************************/



/*
 * mem_alloc
 *
 * This is a wrapper around the platform's memory allocation routine.
 * It will zero out memory before returning it.
 *
 * It will return NULL on failure.
 */
void *mem_alloc(int size)
{
    if(size <= 0) {
        pdebug(DEBUG_WARN, "Allocation size must be greater than zero bytes!");
        return NULL;
    }

    return calloc((size_t)(unsigned int)size, 1);
}



/*
 * mem_realloc
 *
 * This is a wrapper around the platform's memory re-allocation routine.
 *
 * It will return NULL on failure.
 */
void *mem_realloc(void *orig, int size)
{
    if(size <= 0) {
        pdebug(DEBUG_WARN, "New allocation size must be greater than zero bytes!");
        return NULL;
    }

    return realloc(orig, (size_t)(ssize_t)size);
}



/*
 * mem_free
 *
 * Free the allocated memory passed in.  If the passed pointer is
 * null, do nothing.
 */
void mem_free(const void *mem)
{
    if(mem) {
        free((void *)mem);
    }
}




/*
 * mem_set
 *
 * set memory to the passed argument.
 */
void mem_set(void *dest, int c, int size)
{
    if(!dest) {
        pdebug(DEBUG_WARN, "Destination pointer is NULL!");
        return;
    }

    if(size <= 0) {
        pdebug(DEBUG_WARN, "Size to set must be a positive number!");
        return;
    }

    memset(dest, c, (size_t)(ssize_t)size);
}





/*
 * mem_copy
 *
 * copy memory from one pointer to another for the passed number of bytes.
 */
void mem_copy(void *dest, void *src, int size)
{
    if(!dest) {
        pdebug(DEBUG_WARN, "Destination pointer is NULL!");
        return;
    }

    if(!src) {
        pdebug(DEBUG_WARN, "Source pointer is NULL!");
        return;
    }

    if(size < 0) {
        pdebug(DEBUG_WARN, "Size to copy must be a positive number!");
        return;
    }

    if(size == 0) {
        /* nothing to do. */
        return;
    }

    memcpy(dest, src, (size_t)(unsigned int)size);
}



/*
 * mem_move
 *
 * move memory from one pointer to another for the passed number of bytes.
 */
void mem_move(void *dest, void *src, int size)
{
    if(!dest) {
        pdebug(DEBUG_WARN, "Destination pointer is NULL!");
        return;
    }

    if(!src) {
        pdebug(DEBUG_WARN, "Source pointer is NULL!");
        return;
    }

    if(size < 0) {
        pdebug(DEBUG_WARN, "Size to move must be a positive number!");
        return;
    }

    if(size == 0) {
        /* nothing to do. */
        return;
    }

    memmove(dest, src, (size_t)(unsigned int)size);
}





int mem_cmp(void *src1, int src1_size, void *src2, int src2_size)
{
    if(!src1 || src1_size <= 0) {
        if(!src2 || src2_size <= 0) {
            /* both are NULL or zero length, but that is "equal" for our purposes. */
            return 0;
        } else {
            /* first one is "less" than second. */
            return -1;
        }
    } else {
        if(!src2 || src2_size <= 0) {
            /* first is "greater" than second */
            return 1;
        } else {
            /* both pointers are non-NULL and the lengths are positive. */

            /* short circuit the comparison if the blocks are different lengths */
            if(src1_size != src2_size) {
                return (src1_size - src2_size);
            }

            return memcmp(src1, src2, (size_t)(unsigned int)src1_size);
        }
    }
}






/***************************************************************************
 ******************************* Strings ***********************************
 **************************************************************************/


/*
 * str_cmp
 *
 * Return -1, 0, or 1 depending on whether the first string is "less" than the
 * second, the same as the second, or "greater" than the second.  This routine
 * just passes through to POSIX strcmp.
 *
 * Handle edge cases when NULL or zero length strings are passed.
 */
int str_cmp(const char *first, const char *second)
{
    int first_zero = !str_length(first);
    int second_zero = !str_length(second);

    if(first_zero) {
        if(second_zero) {
            pdebug(DEBUG_DETAIL, "NULL or zero length strings passed.");
            return 0;
        } else {
            /* first is "less" than second. */
            return -1;
        }
    } else {
        if(second_zero) {
            /* first is "more" than second. */
            return 1;
        } else {
            /* both are non-zero length. */
            return strcmp(first, second);
        }
    }
}




/*
 * str_cmp_i
 *
 * Returns -1, 0, or 1 depending on whether the first string is "less" than the
 * second, the same as the second, or "greater" than the second.  The comparison
 * is done case insensitive.
 *
 * Handle the usual edge cases.
 */
int str_cmp_i(const char *first, const char *second)
{
    int first_zero = !str_length(first);
    int second_zero = !str_length(second);

    if(first_zero) {
        if(second_zero) {
            pdebug(DEBUG_DETAIL, "NULL or zero length strings passed.");
            return 0;
        } else {
            /* first is "less" than second. */
            return -1;
        }
    } else {
        if(second_zero) {
            /* first is "more" than second. */
            return 1;
        } else {
            /* both are non-zero length. */
            return strcasecmp(first,second);
        }
    }
}



/*
 * str_cmp_i_n
 *
 * Returns -1, 0, or 1 depending on whether the first string is "less" than the
 * second, the same as the second, or "greater" than the second.  The comparison
 * is done case insensitive.  Compares only the first count characters.
 *
 * It just passes this through to POSIX strncasecmp.
 */
int str_cmp_i_n(const char *first, const char *second, int count)
{
    int first_zero = !str_length(first);
    int second_zero = !str_length(second);

    if(count < 0) {
        pdebug(DEBUG_WARN, "Illegal negative count!");
        return -1;
    }

    if(count == 0) {
        pdebug(DEBUG_DETAIL, "Called with comparison count of zero!");
        return 0;
    }

    if(first_zero) {
        if(second_zero) {
            pdebug(DEBUG_DETAIL, "NULL or zero length strings passed.");
            return 0;
        } else {
            /* first is "less" than second. */
            return -1;
        }
    } else {
        if(second_zero) {
            /* first is "more" than second. */
            return 1;
        } else {
            /* both are non-zero length. */
            return strncasecmp(first, second, (size_t)(unsigned int)count);
        }
    }
    return strncasecmp(first, second, (size_t)(unsigned int)count);
}



/*
 * str_str_cmp_i
 *
 * Returns a pointer to the location of the needle string in the haystack string
 * or NULL if there is no match.  The comparison is done case-insensitive.
 *
 * Handle the usual edge cases.
 */
char *str_str_cmp_i(const char *haystack, const char *needle)
{
    int haystack_zero = !str_length(haystack);
    int needle_zero = !str_length(needle);

    if(haystack_zero) {
        pdebug(DEBUG_DETAIL, "Haystack string is NULL or zero length.");
        return NULL;
    }

    if(needle_zero) {
        pdebug(DEBUG_DETAIL, "Needle string is NULL or zero length.");
        return NULL;
    }

    return strcasestr(haystack, needle);
}



/*
 * str_copy
 *
 * Returns
 */
int str_copy(char *dst, int dst_size, const char *src)
{
    if (!dst) {
        pdebug(DEBUG_WARN, "Destination string pointer is NULL!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if (!src) {
        pdebug(DEBUG_WARN, "Source string pointer is NULL!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(dst_size <= 0) {
        pdebug(DEBUG_WARN, "Destination size is negative or zero!");
        return PLCTAG_ERR_TOO_SMALL;
    }

    strncpy(dst, src, (size_t)(unsigned int)dst_size);
    return PLCTAG_STATUS_OK;
}


/*
 * str_length
 *
 * Return the length of the string.  If a null pointer is passed, return
 * null.
 */
int str_length(const char *str)
{
    if(!str) {
        return 0;
    }

    return (int)strlen(str);
}




/*
 * str_dup
 *
 * Copy the passed string and return a pointer to the copy.
 * The caller is responsible for freeing the memory.
 */
char *str_dup(const char *str)
{
    if(!str) {
        return NULL;
    }

    return strdup(str);
}



/*
 * str_to_int
 *
 * Convert the characters in the passed string into
 * an int.  Return an int in integer in the passed
 * pointer and a status from the function.
 */
int str_to_int(const char *str, int *val)
{
    char *endptr;
    long int tmp_val;

    tmp_val = strtol(str,&endptr,0);

    if (errno == ERANGE && (tmp_val == LONG_MAX || tmp_val == LONG_MIN)) {
        pdebug(DEBUG_WARN,"strtol returned %ld with errno %d",tmp_val, errno);
        return -1;
    }

    if (endptr == str) {
        return -1;
    }

    /* FIXME - this will truncate long values. */
    *val = (int)tmp_val;

    return 0;
}


int str_to_float(const char *str, float *val)
{
    char *endptr;
    float tmp_val;

    tmp_val = strtof(str,&endptr);

    if (errno == ERANGE && (tmp_val == HUGE_VALF || tmp_val == -HUGE_VALF || tmp_val == 0)) {
        return -1;
    }

    if (endptr == str) {
        return -1;
    }

    /* FIXME - this will truncate long values. */
    *val = tmp_val;

    return 0;
}


char **str_split(const char *str, const char *sep)
{
    int sub_str_count=0;
    int size = 0;
    const char *sub;
    const char *tmp;
    char **res;

    /* first, count the sub strings */
    tmp = str;
    sub = strstr(tmp,sep);

    while(sub && *sub) {
        /* separator could be at the front, ignore that. */
        if(sub != tmp) {
            sub_str_count++;
        }

        tmp = sub + str_length(sep);
        sub = strstr(tmp,sep);
    }

    if(tmp && *tmp && (!sub || !*sub))
        sub_str_count++;

    /* calculate total size for string plus pointers */
    size = ((int)sizeof(char *)*(sub_str_count+1)+str_length(str)+1);

    /* allocate enough memory */
    res = mem_alloc(size);

    if(!res)
        return NULL;

    /* calculate the beginning of the string */
    tmp = (char *)res + sizeof(char *) * (size_t)(sub_str_count+1);

    /* copy the string into the new buffer past the first part with the array of char pointers. */
    str_copy((char *)tmp, (int)(size - ((char*)tmp - (char*)res)), str);

    /* set up the pointers */
    sub_str_count=0;
    sub = strstr(tmp,sep);

    while(sub && *sub) {
        /* separator could be at the front, ignore that. */
        if(sub != tmp) {
            /* store the pointer */
            res[sub_str_count] = (char *)tmp;

            sub_str_count++;
        }

        /* zero out the separator chars */
        mem_set((char*)sub,0,str_length(sep));

        /* point past the separator (now zero) */
        tmp = sub + str_length(sep);

        /* find the next separator */
        sub = strstr(tmp,sep);
    }

    /* if there is a chunk at the end, store it. */
    if(tmp && *tmp && (!sub || !*sub)) {
        res[sub_str_count] = (char*)tmp;
    }

    return res;
}



char *str_concat_impl(int num_args, ...)
{
    va_list arg_list;
    int total_length = 0;
    char *result = NULL;
    char *tmp = NULL;

    /* first loop to find the length */
    va_start(arg_list, num_args);
    for(int i=0; i < num_args; i++) {
        tmp = va_arg(arg_list, char *);
        if(tmp) {
            total_length += str_length(tmp);
        }
    }
    va_end(arg_list);

    /* make a buffer big enough */
    total_length += 1;

    result = mem_alloc(total_length);
    if(!result) {
        pdebug(DEBUG_ERROR,"Unable to allocate new string buffer!");
        return NULL;
    }

    /* loop to copy the strings */
    result[0] = 0;
    va_start(arg_list, num_args);
    for(int i=0; i < num_args; i++) {
        tmp = va_arg(arg_list, char *);
        if(tmp) {
            int len = str_length(result);
            str_copy(&result[len], total_length - len, tmp);
        }
    }
    va_end(arg_list);

    return result;
}




/***************************************************************************
 ******************************* Mutexes ***********************************
 **************************************************************************/

struct mutex_t {
    pthread_mutex_t p_mutex;
    int initialized;
};

int mutex_create(mutex_p *m)
{
    pthread_mutexattr_t mutex_attribs;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(*m) {
        pdebug(DEBUG_WARN, "Called with non-NULL pointer!");
    }

    *m = (struct mutex_t *)mem_alloc(sizeof(struct mutex_t));

    if(! *m) {
        pdebug(DEBUG_ERROR,"null mutex pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* set up for recursive locking. */
    pthread_mutexattr_init(&mutex_attribs);
    pthread_mutexattr_settype(&mutex_attribs, PTHREAD_MUTEX_RECURSIVE);

    if(pthread_mutex_init(&((*m)->p_mutex),&mutex_attribs)) {
        pthread_mutexattr_destroy(&mutex_attribs);
        mem_free(*m);
        *m = NULL;
        pdebug(DEBUG_ERROR,"Error initializing mutex.");
        return PLCTAG_ERR_MUTEX_INIT;
    }

    (*m)->initialized = 1;

    pthread_mutexattr_destroy(&mutex_attribs);

    pdebug(DEBUG_DETAIL, "Done creating mutex %p.", *m);

    return PLCTAG_STATUS_OK;
}


int mutex_lock_impl(const char *func, int line, mutex_p m)
{
    pdebug(DEBUG_SPEW,"locking mutex %p, called from %s:%d.", m, func, line);

    if(!m) {
        pdebug(DEBUG_WARN, "null mutex pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!m->initialized) {
        return PLCTAG_ERR_MUTEX_INIT;
    }

    if(pthread_mutex_lock(&(m->p_mutex))) {
        pdebug(DEBUG_WARN, "error locking mutex.");
        return PLCTAG_ERR_MUTEX_LOCK;
    }

    //pdebug(DEBUG_SPEW,"Done.");

    return PLCTAG_STATUS_OK;
}


int mutex_try_lock_impl(const char *func, int line, mutex_p m)
{
    pdebug(DEBUG_SPEW,"trying to lock mutex %p, called from %s:%d.", m, func, line);

    if(!m) {
        pdebug(DEBUG_WARN, "null mutex pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!m->initialized) {
        return PLCTAG_ERR_MUTEX_INIT;
    }

    if(pthread_mutex_trylock(&(m->p_mutex))) {
        pdebug(DEBUG_SPEW, "error locking mutex.");
        return PLCTAG_ERR_MUTEX_LOCK;
    }

    /*pdebug(DEBUG_DETAIL,"Done.");*/

    return PLCTAG_STATUS_OK;
}



int mutex_unlock_impl(const char *func, int line, mutex_p m)
{
    pdebug(DEBUG_SPEW,"unlocking mutex %p, called from %s:%d.", m, func, line);

    if(!m) {
        pdebug(DEBUG_WARN,"null mutex pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!m->initialized) {
        return PLCTAG_ERR_MUTEX_INIT;
    }

    if(pthread_mutex_unlock(&(m->p_mutex))) {
        pdebug(DEBUG_WARN, "error unlocking mutex.");
        return PLCTAG_ERR_MUTEX_UNLOCK;
    }

    //pdebug(DEBUG_SPEW,"Done.");

    return PLCTAG_STATUS_OK;
}


int mutex_destroy(mutex_p *m)
{
    pdebug(DEBUG_DETAIL, "Starting to destroy mutex %p.", m);

    if(!m || !*m) {
        pdebug(DEBUG_WARN, "null mutex pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(pthread_mutex_destroy(&((*m)->p_mutex))) {
        pdebug(DEBUG_WARN, "error while attempting to destroy mutex.");
        return PLCTAG_ERR_MUTEX_DESTROY;
    }

    mem_free(*m);

    *m = NULL;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}







/***************************************************************************
 ******************************* Threads ***********************************
 **************************************************************************/

struct thread_t {
    pthread_t p_thread;
    int initialized;
};

/*
 * thread_create()
 *
 * Start up a new thread.  This allocates the thread_t structure and starts
 * the passed function.  The arg argument is passed to the function.
 *
 * TODO - use the stacksize!
 */

int thread_create(thread_p *t, thread_func_t func, int stacksize, void *arg)
{
    pdebug(DEBUG_DETAIL, "Starting.");

    pdebug(DEBUG_DETAIL, "Warning: ignoring stacksize (%d) parameter.", stacksize);

    if(!t) {
        pdebug(DEBUG_WARN, "null thread pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    *t = (thread_p)mem_alloc(sizeof(struct thread_t));

    if(! *t) {
        pdebug(DEBUG_ERROR, "Failed to allocate memory for thread.");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* create a pthread.  0 means success. */
    if(pthread_create(&((*t)->p_thread), NULL, func, arg)) {
        pdebug(DEBUG_ERROR, "error creating thread.");
        return PLCTAG_ERR_THREAD_CREATE;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}


/*
 * platform_thread_stop()
 *
 * Stop the current thread.  Does not take arguments.  Note: the thread
 * ends completely and this function does not return!
 */
void thread_stop(void)
{
    pthread_exit((void*)0);
}


/*
 * thread_kill()
 *
 * Kill another thread.
 */

void thread_kill(thread_p t)
{
    if(t) {
#ifdef __ANDROID__
        pthread_kill(t->p_thread, 0);
#else
        pthread_cancel(t->p_thread);
#endif /* __ANDROID__ */
    }
}

/*
 * thread_join()
 *
 * Wait for the argument thread to stop and then continue.
 */

int thread_join(thread_p t)
{
    void *unused;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!t) {
        pdebug(DEBUG_WARN, "null thread pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(pthread_join(t->p_thread,&unused)) {
        pdebug(DEBUG_ERROR, "Error joining thread.");
        return PLCTAG_ERR_THREAD_JOIN;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}


/*
 * thread_detach
 *
 * Detach the thread.  You cannot call thread_join on a detached thread!
 */

int thread_detach()
{
    pthread_detach(pthread_self());

    return PLCTAG_STATUS_OK;
}



/*
 * thread_destroy
 *
 * This gets rid of the resources of a thread struct.  The thread in
 * question must be dead first!
 */
int thread_destroy(thread_p *t)
{
    pdebug(DEBUG_DETAIL, "Starting.");

    if(!t || ! *t) {
        pdebug(DEBUG_WARN, "null thread pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    mem_free(*t);

    *t = NULL;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}






/***************************************************************************
 ******************************* Atomic Ops ********************************
 **************************************************************************/

/*
 * lock_acquire
 *
 * Tries to write a non-zero value into the lock atomically.
 *
 * Returns non-zero on success.
 *
 * Warning: do not pass null pointers!
 */

#define ATOMIC_LOCK_VAL (1)

int lock_acquire_try(lock_t *lock)
{
    int rc = __sync_lock_test_and_set((int*)lock, ATOMIC_LOCK_VAL);

    if(rc != ATOMIC_LOCK_VAL) {
        return 1;
    } else {
        return 0;
    }
}

int lock_acquire(lock_t *lock)
{
    while(!lock_acquire_try(lock)) ;

    return 1;
}


void lock_release(lock_t *lock)
{
    __sync_lock_release((int*)lock);
    /*pdebug("released lock");*/
}


/***************************************************************************
 ************************* Condition Variables *****************************
 ***************************************************************************/

struct cond_t {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int flag;
};

int cond_create(cond_p *c)
{
    int rc = PLCTAG_STATUS_OK;
    cond_p tmp_cond = NULL;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!c) {
        pdebug(DEBUG_WARN, "Null pointer to condition var pointer!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(*c) {
        pdebug(DEBUG_WARN, "Condition var pointer is not null, was it not deleted first?");
    }

    /* clear the output first. */
    *c = NULL;

    tmp_cond = mem_alloc((int)(unsigned int)sizeof(*tmp_cond));
    if(!tmp_cond) {
        pdebug(DEBUG_WARN, "Unable to allocate new condition var!");
        return PLCTAG_ERR_NO_MEM;
    }

    if(pthread_mutex_init(&(tmp_cond->mutex), NULL)) {
        pdebug(DEBUG_WARN, "Unable to initialize pthread mutex!");
        mem_free(tmp_cond);
        return PLCTAG_ERR_CREATE;
    }

    if(pthread_cond_init(&(tmp_cond->cond), NULL)) {
        pdebug(DEBUG_WARN, "Unable to initialize pthread condition var!");
        pthread_mutex_destroy(&(tmp_cond->mutex));
        mem_free(tmp_cond);
        return PLCTAG_ERR_CREATE;
    }

    tmp_cond->flag = 0;

    *c = tmp_cond;

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}


int cond_wait_impl(const char *func, int line_num, cond_p c, int timeout_ms)
{
    int rc = PLCTAG_STATUS_OK;
    int64_t start_time = time_ms();
    int64_t end_time = start_time + timeout_ms;
    struct timespec timeout;

    pdebug(DEBUG_SPEW, "Starting. Called from %s:%d.", func, line_num);

    if(!c) {
        pdebug(DEBUG_WARN, "Condition var pointer is null in call from %s:%d!", func, line_num);
        return PLCTAG_ERR_NULL_PTR;
    }

    if(timeout_ms <= 0) {
        pdebug(DEBUG_WARN, "Timeout must be a positive value but was %d in call from %s:%d!", timeout_ms, func, line_num);
        return PLCTAG_ERR_BAD_PARAM;
    }

    if(pthread_mutex_lock(& (c->mutex))) {
        pdebug(DEBUG_WARN, "Unable to lock mutex!");
        return PLCTAG_ERR_MUTEX_LOCK;
    }

    /*
     * set up timeout.
     *
     * NOTE: the time is _ABSOLUTE_!  This is not a relative delay.
     */
    timeout.tv_sec = (time_t)(end_time / 1000);
    timeout.tv_nsec = (long)1000000 * (long)(end_time % 1000);

    while(!c->flag) {
        int64_t time_left = (int64_t)timeout_ms - (time_ms() - start_time);

        pdebug(DEBUG_SPEW, "Waiting for %" PRId64 "ms.", time_left);

        if(time_left > 0) {
            int wait_rc = 0;

            wait_rc = pthread_cond_timedwait(&(c->cond), &(c->mutex), &timeout);
            if(wait_rc == ETIMEDOUT) {
                pdebug(DEBUG_SPEW, "Timeout response from condition var wait.");
                rc = PLCTAG_ERR_TIMEOUT;
                break;
            } else if(wait_rc != 0) {
                pdebug(DEBUG_WARN, "Error %d waiting on condition variable!", errno);
                rc = PLCTAG_ERR_BAD_STATUS;
                break;
            } else {
                /* we might need to wait again. could be a spurious wake up. */
                pdebug(DEBUG_SPEW, "Condition var wait returned.");
                rc = PLCTAG_STATUS_OK;
            }
        } else {
            pdebug(DEBUG_DETAIL, "Timed out.");
            rc = PLCTAG_ERR_TIMEOUT;
            break;
        }
    }

    if(c->flag) {
        pdebug(DEBUG_SPEW, "Condition var signaled for call at %s:%d.", func, line_num);

        /* clear the flag now that we've responded. */
        c->flag = 0;
    } else {
        pdebug(DEBUG_SPEW, "Condition wait terminated due to error or timeout for call at %s:%d.", func, line_num);
    }

    if(pthread_mutex_unlock(& (c->mutex))) {
        pdebug(DEBUG_WARN, "Unable to unlock mutex!");
        return PLCTAG_ERR_MUTEX_UNLOCK;
    }

    pdebug(DEBUG_SPEW, "Done for call at %s:%d.", func, line_num);

    return rc;
}


int cond_signal_impl(const char *func, int line_num, cond_p c)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW, "Starting.  Called from %s:%d.", func, line_num);

    if(!c) {
        pdebug(DEBUG_WARN, "Condition var pointer is null in call at %s:%d!", func, line_num);
        return PLCTAG_ERR_NULL_PTR;
    }

    if(pthread_mutex_lock(& (c->mutex))) {
        pdebug(DEBUG_WARN, "Unable to lock mutex!");
        return PLCTAG_ERR_MUTEX_LOCK;
    }

    c->flag = 1;

    if(pthread_cond_signal(&(c->cond))) {
        pdebug(DEBUG_WARN, "Signal of condition var returned error %d in call at %s:%d!", errno, func, line_num);
        rc = PLCTAG_ERR_BAD_STATUS;
    }

    if(pthread_mutex_unlock(& (c->mutex))) {
        pdebug(DEBUG_WARN, "Unable to unlock mutex!");
        return PLCTAG_ERR_MUTEX_UNLOCK;
    }

    pdebug(DEBUG_SPEW, "Done. Called from %s:%d.", func, line_num);

    return rc;
}


int cond_clear_impl(const char *func, int line_num, cond_p c)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW, "Starting.  Called from %s:%d.", func, line_num);

    if(!c) {
        pdebug(DEBUG_WARN, "Condition var pointer is null in call at %s:%d!", func, line_num);
        return PLCTAG_ERR_NULL_PTR;
    }

    if(pthread_mutex_lock(& (c->mutex))) {
        pdebug(DEBUG_WARN, "Unable to lock mutex!");
        return PLCTAG_ERR_MUTEX_LOCK;
    }

    c->flag = 0;

    if(pthread_mutex_unlock(& (c->mutex))) {
        pdebug(DEBUG_WARN, "Unable to unlock mutex!");
        return PLCTAG_ERR_MUTEX_UNLOCK;
    }

    pdebug(DEBUG_SPEW, "Done. Called from %s:%d.", func, line_num);

    return rc;
}


int cond_destroy(cond_p *c)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!c || ! *c) {
        pdebug(DEBUG_WARN, "Condition var pointer is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    pthread_cond_destroy(&((*c)->cond));
    pthread_mutex_destroy(&((*c)->mutex));

    mem_free(*c);

    *c = NULL;

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}



/***************************************************************************
 ******************************* Sockets ***********************************
 **************************************************************************/


#ifndef INVALID_SOCKET
#define INVALID_SOCKET (-1)
#endif

struct sock_t {
    int fd;
    int wake_read_fd;
    int wake_write_fd;
    int port;
    int is_open;
};


static int sock_create_event_wakeup_channel(sock_p sock);

#define MAX_IPS (8)

int socket_create(sock_p *s)
{
    pdebug(DEBUG_DETAIL, "Starting.");

    if(!s) {
        pdebug(DEBUG_WARN, "null socket pointer.");
        return PLCTAG_ERR_NULL_PTR;
    }

    *s = (sock_p)mem_alloc(sizeof(struct sock_t));

    if(! *s) {
        pdebug(DEBUG_ERROR, "Failed to allocate memory for socket.");
        return PLCTAG_ERR_NO_MEM;
    }

    (*s)->fd = INVALID_SOCKET;
    (*s)->wake_read_fd = INVALID_SOCKET;
    (*s)->wake_write_fd = INVALID_SOCKET;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}


int socket_connect_tcp_start(sock_p s, const char *host, int port)
{
    int rc = PLCTAG_STATUS_OK;
    struct in_addr ips[MAX_IPS];
    int num_ips = 0;
    struct sockaddr_in gw_addr;
    int sock_opt = 1;
    int i = 0;
    int done = 0;
    int fd;
    int flags;
    struct timeval timeout; /* used for timing out connections etc. */
    struct linger so_linger; /* used to set up short/no lingering after connections are close()ed. */

    pdebug(DEBUG_DETAIL,"Starting.");

    /* Open a socket for communication with the gateway. */
    fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    /* check for errors */
    if(fd < 0) {
        pdebug(DEBUG_ERROR,"Socket creation failed, errno: %d",errno);
        return PLCTAG_ERR_OPEN;
    }

    /* set up our socket to allow reuse if we crash suddenly. */
    sock_opt = 1;

    if(setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,(char*)&sock_opt,sizeof(sock_opt))) {
        close(fd);
        pdebug(DEBUG_ERROR, "Error setting socket reuse option, errno: %d",errno);
        return PLCTAG_ERR_OPEN;
    }

#ifdef BSD_OS_TYPE
    /* The *BSD family has a different way to suppress SIGPIPE on sockets. */
    if(setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, (char*)&sock_opt, sizeof(sock_opt))) {
        close(fd);
        pdebug(DEBUG_ERROR, "Error setting socket SIGPIPE suppression option, errno: %d", errno);
        return PLCTAG_ERR_OPEN;
    }
#endif

    timeout.tv_sec = 10;
    timeout.tv_usec = 0;

    if(setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout))) {
        close(fd);
        pdebug(DEBUG_ERROR,"Error setting socket receive timeout option, errno: %d",errno);
        return PLCTAG_ERR_OPEN;
    }

    if(setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char*)&timeout, sizeof(timeout))) {
        close(fd);
        pdebug(DEBUG_ERROR, "Error setting socket set timeout option, errno: %d",errno);
        return PLCTAG_ERR_OPEN;
    }

    /* abort the connection immediately upon close. */
    so_linger.l_onoff = 1;
    so_linger.l_linger = 0;

    if(setsockopt(fd, SOL_SOCKET, SO_LINGER,(char*)&so_linger,sizeof(so_linger))) {
        close(fd);
        pdebug(DEBUG_ERROR,"Error setting socket close linger option, errno: %d",errno);
        return PLCTAG_ERR_OPEN;
    }

    /* make the socket non-blocking. */
    flags=fcntl(fd,F_GETFL,0);
    if(flags<0) {
        pdebug(DEBUG_ERROR, "Error getting socket options, errno: %d", errno);
        close(fd);
        return PLCTAG_ERR_OPEN;
    }

    /* set the non-blocking flag. */
    flags |= O_NONBLOCK;

    if(fcntl(fd,F_SETFL,flags)<0) {
        pdebug(DEBUG_ERROR, "Error setting socket to non-blocking, errno: %d", errno);
        close(fd);
        return PLCTAG_ERR_OPEN;
    }

    /* figure out what address we are connecting to. */

    /* try a numeric IP address conversion first. */
    if(inet_pton(AF_INET,host,(struct in_addr *)ips) > 0) {
        pdebug(DEBUG_DETAIL, "Found numeric IP address: %s",host);
        num_ips = 1;
    } else {
        struct addrinfo hints;
        struct addrinfo *res_head = NULL;
        struct addrinfo *res=NULL;
        int rc = 0;

        mem_set(&ips, 0, sizeof(ips));
        mem_set(&hints, 0, sizeof(hints));

        hints.ai_socktype = SOCK_STREAM; /* TCP */
        hints.ai_family = AF_INET; /* IP V4 only */

        if ((rc = getaddrinfo(host, NULL, &hints, &res_head)) != 0) {
            pdebug(DEBUG_WARN,"Error looking up PLC IP address %s, error = %d\n", host, rc);

            if(res_head) {
                freeaddrinfo(res_head);
            }

            close(fd);
            return PLCTAG_ERR_BAD_GATEWAY;
        }

        res = res_head;
        for(num_ips = 0; res && num_ips < MAX_IPS; num_ips++) {
            ips[num_ips].s_addr = ((struct sockaddr_in *)(res->ai_addr))->sin_addr.s_addr;
            res = res->ai_next;
        }

        freeaddrinfo(res_head);
    }

    pdebug(DEBUG_DETAIL, "Setting up wake pipe.");
    rc = sock_create_event_wakeup_channel(s);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to create wake pipe, error %s!", plc_tag_decode_error(rc));
        return rc;
    }

    /* now try to connect to the remote gateway.  We may need to
     * try several of the IPs we have.
     */

    i = 0;
    done = 0;

    memset((void *)&gw_addr,0, sizeof(gw_addr));
    gw_addr.sin_family = AF_INET ;
    gw_addr.sin_port = htons((uint16_t)port);

    do {
        /* try each IP until we run out or get a connection started. */
        gw_addr.sin_addr.s_addr = ips[i].s_addr;

        pdebug(DEBUG_DETAIL, "Attempting to connect to %s:%d", inet_ntoa(*((struct in_addr *)&ips[i])), port);

        /* this is done non-blocking. Could be interrupted, so restart if needed.*/
        do {
            rc = connect(fd,(struct sockaddr *)&gw_addr,sizeof(gw_addr));
        } while(rc < 0 && errno == EINTR);

        if(rc == 0) {
            /* instantly connected. */
            pdebug(DEBUG_DETAIL, "Connected instantly to %s:%d.", inet_ntoa(*((struct in_addr *)&ips[i])), port);
            done = 1;
            rc = PLCTAG_STATUS_OK;
        } else if(rc < 0 && (errno == EINPROGRESS)) {
            /* the connection has started. */
            pdebug(DEBUG_DETAIL, "Started connecting to %s:%d successfully.", inet_ntoa(*((struct in_addr *)&ips[i])), port);
            done = 1;
            rc = PLCTAG_STATUS_PENDING;
        } else  {
            pdebug(DEBUG_DETAIL, "Attempt to connect to %s:%d failed, errno: %d", inet_ntoa(*((struct in_addr *)&ips[i])),port, errno);
            i++;
        }
    } while(!done && i < num_ips);

    if(!done) {
        close(fd);
        pdebug(DEBUG_ERROR, "Unable to connect to any gateway host IP address!");
        return PLCTAG_ERR_OPEN;
    }

    /* save the values */
    s->fd = fd;
    s->port = port;

    s->is_open = 1;

    pdebug(DEBUG_DETAIL, "Done with status %s.", plc_tag_decode_error(rc));

    return rc;
}



int socket_connect_tcp_check(sock_p sock, int timeout_ms)
{
    int rc = PLCTAG_STATUS_OK;
    fd_set write_set;
    struct timeval tv;
    int select_rc = 0;
    int sock_err = 0;
    socklen_t sock_err_len = (socklen_t)(sizeof(sock_err));


    pdebug(DEBUG_DETAIL,"Starting.");

    if(!sock) {
        pdebug(DEBUG_WARN, "Null socket pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* wait for the socket to be ready. */
    tv.tv_sec = (time_t)(timeout_ms / 1000);
    tv.tv_usec = (suseconds_t)(timeout_ms % 1000) * (suseconds_t)(1000);

    FD_ZERO(&write_set);

    FD_SET(sock->fd, &write_set);

    select_rc = select(sock->fd+1, NULL, &write_set, NULL, &tv);

    if(select_rc == 1) {
        if(FD_ISSET(sock->fd, &write_set)) {
            pdebug(DEBUG_DETAIL, "Socket is probably connected.");
            rc = PLCTAG_STATUS_OK;
        } else {
            pdebug(DEBUG_WARN, "select() returned but socket is not connected!");
            return PLCTAG_ERR_BAD_REPLY;
        }
    } else if(select_rc == 0) {
        pdebug(DEBUG_DETAIL, "Socket connection not done yet.");
        return PLCTAG_ERR_TIMEOUT;
    } else {
        pdebug(DEBUG_WARN, "select() returned status %d!", select_rc);

        switch(errno) {
            case EBADF: /* bad file descriptor */
                pdebug(DEBUG_WARN, "Bad file descriptor used in select()!");
                return PLCTAG_ERR_OPEN;
                break;

            case EINTR: /* signal was caught, this should not happen! */
                pdebug(DEBUG_WARN, "A signal was caught in select() and this should not happen!");
                return PLCTAG_ERR_OPEN;
                break;

            case EINVAL: /* number of FDs was negative or exceeded the max allowed. */
                pdebug(DEBUG_WARN, "The number of fds passed to select() was negative or exceeded the allowed limit or the timeout is invalid!");
                return PLCTAG_ERR_OPEN;
                break;

            case ENOMEM: /* No mem for internal tables. */
                pdebug(DEBUG_WARN, "Insufficient memory for select() to run!");
                return PLCTAG_ERR_NO_MEM;
                break;

            default:
                pdebug(DEBUG_WARN, "Unexpected socket err %d!", errno);
                return PLCTAG_ERR_OPEN;
                break;
        }
    }

    /* now make absolutely sure that the connection is ready. */
    rc = getsockopt(sock->fd, SOL_SOCKET, SO_ERROR, &sock_err, &sock_err_len);
    if(rc == 0) {
        /* sock_err has the error. */
        switch(sock_err) {
            case 0:
                pdebug(DEBUG_DETAIL, "No error, socket is connected.");
                rc = PLCTAG_STATUS_OK;
                break;

            case EBADF:
                pdebug(DEBUG_WARN, "Socket fd is not valid!");
                return PLCTAG_ERR_OPEN;
                break;

            case EFAULT:
                pdebug(DEBUG_WARN, "The address passed to getsockopt() is not a valid user address!");
                return PLCTAG_ERR_OPEN;
                break;

            case EINVAL:
                pdebug(DEBUG_WARN, "The size of the socket error result is invalid!");
                return PLCTAG_ERR_OPEN;
                break;

            case ENOPROTOOPT:
                pdebug(DEBUG_WARN, "The option SO_ERROR is not understood at the SOL_SOCKET level!");
                return PLCTAG_ERR_OPEN;
                break;

            case ENOTSOCK:
                pdebug(DEBUG_WARN, "The FD is not a socket!");
                return PLCTAG_ERR_OPEN;
                break;

            case ECONNREFUSED:
                pdebug(DEBUG_WARN, "Connection refused!");
                return PLCTAG_ERR_OPEN;
                break;

            default:
                pdebug(DEBUG_WARN, "Unexpected error %d returned!", sock_err);
                return PLCTAG_ERR_OPEN;
                break;
        }
    } else {
        pdebug(DEBUG_WARN, "Error %d getting socket connection status!", errno);
        return PLCTAG_ERR_OPEN;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}



int socket_wait_event(sock_p sock, int events, int timeout_ms)
{
    int result = SOCK_EVENT_NONE;
    fd_set read_set;
    fd_set write_set;
    fd_set err_set;
    int max_fd = 0;
    int num_sockets = 0;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!sock) {
        pdebug(DEBUG_WARN, "Null socket pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!sock->is_open) {
        pdebug(DEBUG_WARN, "Socket is not open!");
        return PLCTAG_ERR_READ;
    }

    if(timeout_ms < 0) {
        pdebug(DEBUG_WARN, "Timeout must be zero or positive!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* check if the mask is empty */
    if(events == 0) {
        pdebug(DEBUG_WARN, "Passed event mask is empty!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* set up fd sets */
    FD_ZERO(&read_set);
    FD_ZERO(&write_set);
    FD_ZERO(&err_set);

    /* calculate the maximum fd */
    max_fd = (sock->fd > sock->wake_read_fd ? sock->fd : sock->wake_read_fd);

    /* add the wake fd */
    FD_SET(sock->wake_read_fd, &read_set);

    /* we always want to know about errors. */
    FD_SET(sock->fd, &err_set);

    /* add more depending on the mask. */
    if(events & SOCK_EVENT_CAN_READ) {
        FD_SET(sock->fd, &read_set);
    }

    if((events & SOCK_EVENT_CONNECT) || (events & SOCK_EVENT_CAN_WRITE)) {
        FD_SET(sock->fd, &write_set);
    }

    /* calculate the timeout. */
    if(timeout_ms > 0) {
        struct timeval tv;

        tv.tv_sec = (time_t)(timeout_ms / 1000);
        tv.tv_usec = (suseconds_t)(timeout_ms % 1000) * (suseconds_t)(1000);

        num_sockets = select(max_fd + 1, &read_set, &write_set, &err_set, &tv);
    } else {
        num_sockets = select(max_fd + 1, &read_set, &write_set, &err_set, NULL);
    }

    if(num_sockets == 0) {
        result |= (events & SOCK_EVENT_TIMEOUT);
    } else if(num_sockets > 0) {
        /* was there a wake up? */
        if(FD_ISSET(sock->wake_read_fd, &read_set)) {
            int bytes_read = 0;
            char buf[32];

            /* empty the socket. */
            while((bytes_read = (int)read(sock->wake_read_fd, &buf[0], sizeof(buf))) > 0) { }

            pdebug(DEBUG_DETAIL, "Socket woken up.");
            result |= (events & SOCK_EVENT_WAKE_UP);
        }

        /* is read ready for the main fd? */
        if(FD_ISSET(sock->fd, &read_set)) {
            char buf;
            int byte_read = 0;

            byte_read = (int)recv(sock->fd, &buf, sizeof(buf), MSG_PEEK);

            if(byte_read) {
                pdebug(DEBUG_DETAIL, "Socket can read.");
                result |= (events & SOCK_EVENT_CAN_READ);
            } else {
                pdebug(DEBUG_DETAIL, "Socket disconnected.");
                result |= (events & SOCK_EVENT_DISCONNECT);
            }
        }

        /* is write ready for the main fd? */
        if(FD_ISSET(sock->fd, &write_set)) {
            pdebug(DEBUG_DETAIL, "Socket can write or just connected.");
            result |= ((events & SOCK_EVENT_CAN_WRITE) | (events & SOCK_EVENT_CONNECT));
        }

        /* is there an error? */
        if(FD_ISSET(sock->fd, &err_set)) {
            pdebug(DEBUG_DETAIL, "Socket has error!");
            result |= (events & SOCK_EVENT_ERROR);
        }
    } else {
        /* error */
        pdebug(DEBUG_WARN, "select() returned status %d!", num_sockets);

        switch(errno) {
            case EBADF: /* bad file descriptor */
                pdebug(DEBUG_WARN, "Bad file descriptor used in select()!");
                return PLCTAG_ERR_BAD_PARAM;
                break;

            case EINTR: /* signal was caught, this should not happen! */
                pdebug(DEBUG_WARN, "A signal was caught in select() and this should not happen!");
                return PLCTAG_ERR_BAD_CONFIG;
                break;

            case EINVAL: /* number of FDs was negative or exceeded the max allowed. */
                pdebug(DEBUG_WARN, "The number of fds passed to select() was negative or exceeded the allowed limit or the timeout is invalid!");
                return PLCTAG_ERR_BAD_PARAM;
                break;

            case ENOMEM: /* No mem for internal tables. */
                pdebug(DEBUG_WARN, "Insufficient memory for select() to run!");
                return PLCTAG_ERR_NO_MEM;
                break;

            default:
                pdebug(DEBUG_WARN, "Unexpected socket err %d!", errno);
                return PLCTAG_ERR_BAD_STATUS;
                break;
        }
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return result;
}


int socket_wake(sock_p sock)
{
    int rc = PLCTAG_STATUS_OK;
    const char dummy_data[] = "Dummy data.";

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!sock) {
        pdebug(DEBUG_WARN, "Null socket pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!sock->is_open) {
        pdebug(DEBUG_WARN, "Socket is not open!");
        return PLCTAG_ERR_READ;
    }

    // rc = (int)write(sock->wake_write_fd, &dummy_data[0], sizeof(dummy_data));
#ifdef BSD_OS_TYPE
    /* On *BSD and macOS, the socket option is set to prevent SIGPIPE. */
    rc = (int)write(sock->wake_write_fd, &dummy_data[0], sizeof(dummy_data));
#else
    /* on Linux, we use MSG_NOSIGNAL */
    rc = (int)send(sock->wake_write_fd, &dummy_data[0], sizeof(dummy_data), MSG_NOSIGNAL);
#endif
    if(rc >= 0) {
        rc = PLCTAG_STATUS_OK;
    } else {
        pdebug(DEBUG_WARN, "Socket write error: rc=%d, errno=%d", rc, errno);
        return PLCTAG_ERR_WRITE;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}



int socket_read(sock_p s, uint8_t *buf, int size, int timeout_ms)
{
    int rc;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!s) {
        pdebug(DEBUG_WARN, "Socket pointer is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!buf) {
        pdebug(DEBUG_WARN, "Buffer pointer is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!s->is_open) {
        pdebug(DEBUG_WARN, "Socket is not open!");
        return PLCTAG_ERR_READ;
    }

    if(timeout_ms < 0) {
        pdebug(DEBUG_WARN, "Timeout must be zero or positive!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /*
     * Try to read immediately.   If we get data, we skip any other
     * delays.   If we do not, then see if we have a timeout.
     */

    /* The socket is non-blocking. */
    rc = (int)read(s->fd,buf,(size_t)size);
    if(rc < 0) {
        if(errno == EAGAIN || errno == EWOULDBLOCK) {
            if(timeout_ms > 0) {
                pdebug(DEBUG_DETAIL, "Immediate read attempt did not succeed, now wait for select().");
            } else {
                pdebug(DEBUG_DETAIL, "Read resulted in no data.");
            }

            rc = 0;
        } else {
            pdebug(DEBUG_WARN,"Socket read error: rc=%d, errno=%d", rc, errno);
            return PLCTAG_ERR_READ;
        }
    }

    /* only wait if we have a timeout and no error and no data. */
    if(rc == 0 && timeout_ms > 0) {
        fd_set read_set;
        struct timeval tv;
        int select_rc = 0;

        tv.tv_sec = (time_t)(timeout_ms / 1000);
        tv.tv_usec = (suseconds_t)(timeout_ms % 1000) * (suseconds_t)(1000);

        FD_ZERO(&read_set);

        FD_SET(s->fd, &read_set);

        select_rc = select(s->fd+1, &read_set, NULL, NULL, &tv);
        if(select_rc == 1) {
            if(FD_ISSET(s->fd, &read_set)) {
                pdebug(DEBUG_DETAIL, "Socket can read data.");
            } else {
                pdebug(DEBUG_WARN, "select() returned but socket is not ready to read data!");
                return PLCTAG_ERR_BAD_REPLY;
            }
        } else if(select_rc == 0) {
            pdebug(DEBUG_DETAIL, "Socket read timed out.");
            return PLCTAG_ERR_TIMEOUT;
        } else {
            pdebug(DEBUG_WARN, "select() returned status %d!", select_rc);

            switch(errno) {
                case EBADF: /* bad file descriptor */
                    pdebug(DEBUG_WARN, "Bad file descriptor used in select()!");
                    return PLCTAG_ERR_BAD_PARAM;
                    break;

                case EINTR: /* signal was caught, this should not happen! */
                    pdebug(DEBUG_WARN, "A signal was caught in select() and this should not happen!");
                    return PLCTAG_ERR_BAD_CONFIG;
                    break;

                case EINVAL: /* number of FDs was negative or exceeded the max allowed. */
                    pdebug(DEBUG_WARN, "The number of fds passed to select() was negative or exceeded the allowed limit or the timeout is invalid!");
                    return PLCTAG_ERR_BAD_PARAM;
                    break;

                case ENOMEM: /* No mem for internal tables. */
                    pdebug(DEBUG_WARN, "Insufficient memory for select() to run!");
                    return PLCTAG_ERR_NO_MEM;
                    break;

                default:
                    pdebug(DEBUG_WARN, "Unexpected socket err %d!", errno);
                    return PLCTAG_ERR_BAD_STATUS;
                    break;
            }
        }

        /* try to read again. */
        rc = (int)read(s->fd,buf,(size_t)size);
        if(rc < 0) {
            if(errno == EAGAIN || errno == EWOULDBLOCK) {
                pdebug(DEBUG_DETAIL, "No data read.");
                rc = 0;
            } else {
                pdebug(DEBUG_WARN,"Socket read error: rc=%d, errno=%d", rc, errno);
                return PLCTAG_ERR_READ;
            }
        }
    }

    pdebug(DEBUG_DETAIL, "Done: result %d.", rc);

    return rc;
}



int socket_write(sock_p s, uint8_t *buf, int size, int timeout_ms)
{
    int rc;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!s) {
        pdebug(DEBUG_WARN, "Socket pointer is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!buf) {
        pdebug(DEBUG_WARN, "Buffer pointer is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!s->is_open) {
        pdebug(DEBUG_WARN, "Socket is not open!");
        return PLCTAG_ERR_WRITE;
    }

    if(timeout_ms < 0) {
        pdebug(DEBUG_WARN, "Timeout must be zero or positive!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /*
     * Try to write without waiting.
     *
     * In the case that we can immediately write, then we skip a
     * system call to select().   If we cannot, then we will
     * call select().
     */

#ifdef BSD_OS_TYPE
    /* On *BSD and macOS, the socket option is set to prevent SIGPIPE. */
    rc = (int)write(s->fd, buf, (size_t)size);
#else
    /* on Linux, we use MSG_NOSIGNAL */
    rc = (int)send(s->fd, buf, (size_t)size, MSG_NOSIGNAL);
#endif

    if(rc < 0) {
        if(errno == EAGAIN || errno == EWOULDBLOCK) {
            rc = 0;
        } else {
            pdebug(DEBUG_WARN, "Socket write error: rc=%d, errno=%d", rc, errno);
            return PLCTAG_ERR_WRITE;
        }
    }

    /* only wait if we have a timeout and no error and wrote no data. */
    if(rc == 0 && timeout_ms > 0) {
        fd_set write_set;
        struct timeval tv;
        int select_rc = 0;

        tv.tv_sec = (time_t)(timeout_ms / 1000);
        tv.tv_usec = (suseconds_t)(timeout_ms % 1000) * (suseconds_t)(1000);

        FD_ZERO(&write_set);

        FD_SET(s->fd, &write_set);

        select_rc = select(s->fd+1, NULL, &write_set, NULL, &tv);
        if(select_rc == 1) {
            if(FD_ISSET(s->fd, &write_set)) {
                pdebug(DEBUG_DETAIL, "Socket can write data.");
            } else {
                pdebug(DEBUG_WARN, "select() returned but socket is not ready to write data!");
                return PLCTAG_ERR_BAD_REPLY;
            }
        } else if(select_rc == 0) {
            pdebug(DEBUG_DETAIL, "Socket write timed out.");
            return PLCTAG_ERR_TIMEOUT;
        } else {
            pdebug(DEBUG_WARN, "select() returned status %d!", select_rc);

            switch(errno) {
                case EBADF: /* bad file descriptor */
                    pdebug(DEBUG_WARN, "Bad file descriptor used in select()!");
                    return PLCTAG_ERR_BAD_PARAM;
                    break;

                case EINTR: /* signal was caught, this should not happen! */
                    pdebug(DEBUG_WARN, "A signal was caught in select() and this should not happen!");
                    return PLCTAG_ERR_BAD_CONFIG;
                    break;

                case EINVAL: /* number of FDs was negative or exceeded the max allowed. */
                    pdebug(DEBUG_WARN, "The number of fds passed to select() was negative or exceeded the allowed limit or the timeout is invalid!");
                    return PLCTAG_ERR_BAD_PARAM;
                    break;

                case ENOMEM: /* No mem for internal tables. */
                    pdebug(DEBUG_WARN, "Insufficient memory for select() to run!");
                    return PLCTAG_ERR_NO_MEM;
                    break;

                default:
                    pdebug(DEBUG_WARN, "Unexpected socket err %d!", errno);
                    return PLCTAG_ERR_BAD_STATUS;
                    break;
            }
        }

        /* select() passed and said we can write, so try. */
    #ifdef BSD_OS_TYPE
        /* On *BSD and macOS, the socket option is set to prevent SIGPIPE. */
        rc = (int)write(s->fd, buf, (size_t)size);
    #else
        /* on Linux, we use MSG_NOSIGNAL */
        rc = (int)send(s->fd, buf, (size_t)size, MSG_NOSIGNAL);
    #endif

        if(rc < 0) {
            if(errno == EAGAIN || errno == EWOULDBLOCK) {
                pdebug(DEBUG_DETAIL, "No data written.");
                rc = 0;
            } else {
                pdebug(DEBUG_WARN, "Socket write error: rc=%d, errno=%d", rc, errno);
                return PLCTAG_ERR_WRITE;
            }
        }
    }

    pdebug(DEBUG_DETAIL, "Done: result = %d.", rc);

    return rc;
}



int socket_close(sock_p s)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    if(!s) {
        pdebug(DEBUG_WARN, "Socket pointer or pointer to socket pointer is NULL!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(s->wake_read_fd != INVALID_SOCKET) {
        if(close(s->wake_read_fd)) {
            pdebug(DEBUG_WARN, "Error closing read wake socket!");
            rc = PLCTAG_ERR_CLOSE;
        }

        s->wake_read_fd = INVALID_SOCKET;
    }

    if(s->wake_write_fd != INVALID_SOCKET) {
        if(close(s->wake_write_fd)) {
            pdebug(DEBUG_WARN, "Error closing write wake socket!");
            rc = PLCTAG_ERR_CLOSE;
        }

        s->wake_write_fd = INVALID_SOCKET;
    }

    if(s->fd != INVALID_SOCKET) {
        if(close(s->fd)) {
            pdebug(DEBUG_WARN, "Error closing socket!");
            rc = PLCTAG_ERR_CLOSE;
        }

        s->fd = INVALID_SOCKET;
    }

    s->is_open = 0;

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}




int socket_destroy(sock_p *s)
{
    pdebug(DEBUG_INFO, "Starting.");

    if(!s || !*s) {
        pdebug(DEBUG_WARN, "Socket pointer or pointer to socket pointer is NULL!");
        return PLCTAG_ERR_NULL_PTR;
    }

    socket_close(*s);

    mem_free(*s);

    *s = NULL;

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_OK;
}





int sock_create_event_wakeup_channel(sock_p sock)
{
    int rc = PLCTAG_STATUS_OK;
    int flags = 0;
    int wake_fds[2] = { 0 };
#ifdef BSD_OS_TYPE
    int sock_opt=1;
#endif

    pdebug(DEBUG_INFO, "Starting.");

    do {
        /* open the pipe for waking the select wait. */
        // if(pipe(wake_fds)) {
        if((rc = socketpair(PF_LOCAL, SOCK_STREAM, 0, wake_fds))) {
            pdebug(DEBUG_WARN, "Unable to open waker pipe!");
            switch(errno) {
                case EAFNOSUPPORT:
                    pdebug(DEBUG_WARN, "The specified addresss family is not supported on this machine!");
                    break;

                case EFAULT:
                    pdebug(DEBUG_WARN, "The address socket_vector does not specify a valid part of the process address space.");
                    break;

                case EMFILE:
                    pdebug(DEBUG_WARN, "No more file descriptors are available for this process.");
                    break;

                case ENFILE:
                    pdebug(DEBUG_WARN, "No more file descriptors are available for the system.");
                    break;

                case ENOBUFS:
                    pdebug(DEBUG_WARN, "Insufficient resources were available in the system to perform the operation.");
                    break;

                case ENOMEM:
                    pdebug(DEBUG_WARN, "Insufficient memory was available to fulfill the request.");
                    break;

                case EOPNOTSUPP:
                    pdebug(DEBUG_WARN, "The specified protocol does not support creation of socket pairs.");
                    break;

                case EPROTONOSUPPORT:
                    pdebug(DEBUG_WARN, "The specified protocol is not supported on this machine.");
                    break;

                case EPROTOTYPE:
                    pdebug(DEBUG_WARN, "The socket type is not supported by the protocol.");
                    break;

                default:
                    pdebug(DEBUG_WARN, "Unexpected error %d!", errno);
                    break;
            }

            rc = PLCTAG_ERR_BAD_REPLY;
            break;
        }

#ifdef BSD_OS_TYPE
        /* The *BSD family has a different way to suppress SIGPIPE on sockets. */
        if(setsockopt(wake_fds[0], SOL_SOCKET, SO_NOSIGPIPE, (char*)&sock_opt, sizeof(sock_opt))) {
            pdebug(DEBUG_ERROR, "Error setting wake fd read socket SIGPIPE suppression option, errno: %d", errno);
            rc = PLCTAG_ERR_OPEN;
            break;
        }

        if(setsockopt(wake_fds[1], SOL_SOCKET, SO_NOSIGPIPE, (char*)&sock_opt, sizeof(sock_opt))) {
            pdebug(DEBUG_ERROR, "Error setting wake fd write socket SIGPIPE suppression option, errno: %d", errno);
            rc = PLCTAG_ERR_OPEN;
            break;
        }
#endif

        /* make the read pipe fd non-blocking. */
        if((flags = fcntl(wake_fds[0], F_GETFL)) < 0) {
            pdebug(DEBUG_WARN, "Unable to get flags of read socket fd!");
            rc = PLCTAG_ERR_BAD_REPLY;
            break;
        }

        /* set read fd non-blocking */
        flags |= O_NONBLOCK;

        if(fcntl(wake_fds[0], F_SETFL, flags) < 0) {
            pdebug(DEBUG_WARN, "Unable to set flags of read socket fd!");
            rc = PLCTAG_ERR_BAD_REPLY;
            break;
        }

        /* make the write pipe fd non-blocking. */
        if((flags = fcntl(wake_fds[1], F_GETFL)) < 0) {
            pdebug(DEBUG_WARN, "Unable to get flags of write socket fd!");
            rc = PLCTAG_ERR_BAD_REPLY;
            break;
        }

        /* set write fd non-blocking */
        flags |= O_NONBLOCK;

        if(fcntl(wake_fds[1], F_SETFL, flags) < 0) {
            pdebug(DEBUG_WARN, "Unable to set flags of write socket fd!");
            rc = PLCTAG_ERR_BAD_REPLY;
            break;
        }

        sock->wake_read_fd = wake_fds[0];
        sock->wake_write_fd = wake_fds[1];
    } while(0);

    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to open waker socket!");

        if(wake_fds[0] != INVALID_SOCKET) {
            close(wake_fds[0]);
            wake_fds[0] = INVALID_SOCKET;
        }

        if(wake_fds[1] != INVALID_SOCKET) {
            close(wake_fds[1]);
            wake_fds[1] = INVALID_SOCKET;
        }
    } else {
        pdebug(DEBUG_INFO, "Done.");
    }

    return rc;
}






/***************************************************************************
 ***************************** Miscellaneous *******************************
 **************************************************************************/




/*
 * sleep_ms
 *
 * Sleep the passed number of milliseconds.  This handles the case of being
 * interrupted by a signal.
 *
 * TODO - should the signal interrupt handling be done here or in app code?
 */
int sleep_ms(int ms)
{
    struct timespec wait_time;
    struct timespec remainder;
    int done = 1;
    int rc = PLCTAG_STATUS_OK;

    if(ms < 0) {
        pdebug(DEBUG_WARN, "called with negative time %d!", ms);
        return PLCTAG_ERR_BAD_PARAM;
    }

    wait_time.tv_sec = ms/1000;
    wait_time.tv_nsec = ((long)ms % 1000)*1000000; /* convert to nanoseconds */

    do {
        int rc = nanosleep(&wait_time, &remainder);
        if(rc < 0 && errno == EINTR) {
            /* we were interrupted, keep going. */
            wait_time = remainder;
            done = 0;
        } else {
            done = 1;

            if(rc < 0) {
                /* error condition. */
                rc = PLCTAG_ERR_BAD_REPLY;
            }
        }
    } while(!done);

    return rc;


    // rc = nanosleep(&wait_time, &remainder);
    // if(rc < 0 && errno != EINTR) {
    //     rc = PLCTAG_ERR_BAD_REPLY;
    // }

    // return rc;
    // struct timeval tv;

    // tv.tv_sec = ms/1000;
    // tv.tv_usec = (ms % 1000)*1000;

    // return select(0,NULL,NULL,NULL, &tv);
}


/*
 * time_ms
 *
 * Return the current epoch time in milliseconds.
 */
int64_t time_ms(void)
{
    struct timeval tv;

    gettimeofday(&tv,NULL);

    return  ((int64_t)tv.tv_sec*1000)+ ((int64_t)tv.tv_usec/1000);
}

#endif

#endif // __PLATFORM_C__


#ifndef __UTIL_ATOMIC_C__
#define __UTIL_ATOMIC_C__

void atomic_init(atomic_int *a, int new_val)
{
    a->lock = LOCK_INIT;
    a->val = new_val;
}



int atomic_get(atomic_int *a)
{
    int val = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    spin_block(&a->lock) {
        val = a->val;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return val;
}



int atomic_set(atomic_int *a, int new_val)
{
    int old_val = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    spin_block(&a->lock) {
        old_val = a->val;
        a->val = new_val;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return old_val;
}



int atomic_add(atomic_int *a, int other)
{
    int old_val = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    spin_block(&a->lock) {
        old_val = a->val;
        a->val += other;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return old_val;
}


int atomic_compare_and_set(atomic_int *a, int old_val, int new_val)
{
    int ret_val = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    spin_block(&a->lock) {
        ret_val = a->val;

        if(ret_val == old_val) {
            a->val = new_val;
        }
    }

    pdebug(DEBUG_SPEW, "Done.");

    return ret_val;
}

#endif // __UTIL_ATOMIC_C__


#ifndef __UTIL_ATTR_C__
#define __UTIL_ATTR_C__

#include <stdio.h>
#include <string.h>


struct attr_entry_t {
    attr_entry next;
    char *name;
    char *val;
};

struct attr_t {
    attr_entry head;
};





/*
 * find_entry
 *
 * A helper function to find the attr_entry that has the
 * passed name.
 */

attr_entry find_entry(attr a, const char *name)
{
    attr_entry e;

    if(!a)
        return NULL;

    e = a->head;

    if(!e)
        return NULL;

    while(e) {
        if(str_cmp(e->name, name) == 0) {
            return e;
        }

        e = e->next;
    }

    return NULL;
}


/*
 * attr_create
 *
 * Create a new attr structure and return a pointer to it.
 */
attr attr_create()
{
    return (attr)mem_alloc(sizeof(struct attr_t));
}









/*
 * attr_create_from_str
 *
 * Parse the passed string into an attr structure and return a pointer to a list of
 * attr_entry structs.
 *
 * Attribute strings are formatted much like URL arguments:
 * foo=bar&blah=humbug&blorg=42&test=one
 * You cannot, currently, have an "=" or "&" character in the value for an
 * attribute.
 */
attr attr_create_from_str(const char *attr_str)
{
    attr res = NULL;
    char **kv_pairs = NULL;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!str_length(attr_str)) {
        pdebug(DEBUG_WARN, "Attribute string needs to be longer than zero characters!");
        return NULL;
    }

    /* split the string on "&" */
    kv_pairs = str_split(attr_str, "&");
    if(!kv_pairs) {
        pdebug(DEBUG_WARN, "No key-value pairs!");
        return NULL;
    }

    /* set up the attribute list head */
    res = attr_create();
    if(!res) {
        pdebug(DEBUG_ERROR, "Unable to allocate memory for attribute list!");
        mem_free(kv_pairs);
        return NULL;
    }

    /* loop over each key-value pair */
    for(char **kv_pair = kv_pairs; *kv_pair; kv_pair++) {
        /* find the position of the '=' character */
        char *separator = strchr(*kv_pair, '=');
        char *key = *kv_pair;
        char *value = separator;

        pdebug(DEBUG_DETAIL, "Key-value pair \"%s\".", *kv_pair);

        if(separator == NULL) {
            pdebug(DEBUG_WARN, "Attribute string \"%s\" has invalid key-value pair near \"%s\"!", attr_str, *kv_pair);
            mem_free(kv_pairs);
            attr_destroy(res);
            return NULL;
        }

        /* value points to the '=' character.  Step past that for the value. */
        value++;

        /* cut the string at the separator. */
        *separator = (char)0;

        pdebug(DEBUG_DETAIL, "Key-value pair before trimming \"%s\":\"%s\".", key, value);

        /* skip leading spaces in the key */
        while(*key == ' ') {
            key++;
        }

        /* zero out all trailing spaces in the key */
        for(int i=str_length(key) - 1; i > 0 && key[i] == ' '; i--) {
            key[i] = (char)0;
        }

        pdebug(DEBUG_DETAIL, "Key-value pair after trimming \"%s\":\"%s\".", key, value);

        /* check the string lengths */

        if(str_length(key) <= 0) {
            pdebug(DEBUG_WARN, "Attribute string \"%s\" has invalid key-value pair near \"%s\"!  Key must not be zero length!", attr_str, *kv_pair);
            mem_free(kv_pairs);
            attr_destroy(res);
            return NULL;
        }

        if(str_length(value) <= 0) {
            pdebug(DEBUG_WARN, "Attribute string \"%s\" has invalid key-value pair near \"%s\"!  Value must not be zero length!", attr_str, *kv_pair);
            mem_free(kv_pairs);
            attr_destroy(res);
            return NULL;
        }

        /* add the key-value pair to the attribute list */
        if(attr_set_str(res, key, value)) {
            pdebug(DEBUG_WARN, "Unable to add key-value pair \"%s\":\"%s\" to attribute list!", key, value);
            mem_free(kv_pairs);
            attr_destroy(res);
            return NULL;
        }
    }

    if(kv_pairs) {
        mem_free(kv_pairs);
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return res;
}





/*
 * attr_set
 *
 * Set/create a new string attribute
 */
int attr_set_str(attr attrs, const char *name, const char *val)
{
    attr_entry e;

    if(!attrs) {
        return 1;
    }

    /* does the entry exist? */
    e = find_entry(attrs, name);

    /* if we had a match, then delete the existing value and add in the
     * new one.
     *
     * If we had no match, then e is NULL and we need to create a new one.
     */
    if(e) {
        /* we had a match, free any existing value */
        if(e->val) {
            mem_free(e->val);
        }

        /* set up the new value */
        e->val = str_dup(val);
        if(!e->val) {
            /* oops! */
            return 1;
        }
    } else {
        /* no match, need a new entry */
        e = (attr_entry)mem_alloc(sizeof(struct attr_entry_t));

        if(e) {
            e->name = str_dup(name);

            if(!e->name) {
                mem_free(e);
                return 1;
            }

            e->val = str_dup(val);

            if(!e->val) {
                mem_free(e->name);
                mem_free(e);
                return 1;
            }

            /* link it in the list */
            e->next = attrs->head;
            attrs->head = e;
        } else {
            /* allocation failed */
            return 1;
        }
    }

    return 0;
}



int attr_set_int(attr attrs, const char *name, int val)
{
    char buf[64];

    snprintf_platform(buf, sizeof buf, "%d", val);

    return attr_set_str(attrs, name, buf);
}



int attr_set_float(attr attrs, const char *name, float val)
{
    char buf[64];

    snprintf_platform(buf, sizeof buf, "%f", val);

    return attr_set_str(attrs, name, buf);
}





/*
 * attr_get
 *
 * Walk the list of attrs and return the value found with the passed name.
 * If the name is not found, return the passed default value.
 */
const char *attr_get_str(attr attrs, const char *name, const char *def)
{
    attr_entry e;

    if(!attrs) {
        return def;
    }

    e = find_entry(attrs, name);

    /* only return a value if there is one. */
    if(e) {
        return e->val;
    } else {
        return def;
    }
}


int attr_get_int(attr attrs, const char *name, int def)
{
    int res;
    int rc;

    const char *str_val = attr_get_str(attrs,name, NULL);

    if(!str_val) {
        return def;
    }

    rc = str_to_int(str_val, &res);

    if(rc) {
        /* format error? */
        return def;
    } else {
        return res;
    }
}


float attr_get_float(attr attrs, const char *name, float def)
{
    float res;
    int rc;

    const char *str_val = attr_get_str(attrs,name, NULL);

    if(!str_val) {
        return def;
    }

    rc = str_to_float(str_val, &res);

    if(rc) {
        /* format error? */
        return def;
    } else {
        return res;
    }
}


int attr_remove(attr attrs, const char *name)
{
    attr_entry e, p;

    if(!attrs)
        return 0;

    e = attrs->head;

    /* no such entry, return */
    if(!e)
        return 0;

    /* loop to find the entry */
    p = NULL;

    while(e) {
        if(str_cmp(e->name, name) == 0) {
            break;
        }

        p = e;
        e = e->next;
    }

    if(e) {
        /* unlink the node */
        if(!p) {
            attrs->head = e->next;
        } else {
            p->next = e->next;
        }

        if(e->name) {
            mem_free(e->name);
        }

        if(e->val) {
            mem_free(e->val);
        }

        mem_free(e);
    } /* else not found */

    return 0;
}


/*
 * attr_delete
 *
 * Destroy and free all memory for an attribute list.
 */
void attr_destroy(attr a)
{
    attr_entry e, p;

    if(!a)
        return;

    e = a->head;

    /* walk down the entry list and free as we go. */
    while(e) {
        if(e->name) {
            mem_free(e->name);
        }

        if(e->val) {
            mem_free(e->val);
        }

        p = e;
        e = e->next;

        mem_free(p);
    }

    mem_free(a);
}

#endif // __UTIL_ATTR_C__


#ifndef __UTIL_DEBUG_C__
#define __UTIL_DEBUG_C__

#include <inttypes.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <string.h>


/*
 * Debugging support.
 */


static int global_debug_level = DEBUG_NONE;
static lock_t thread_num_lock = LOCK_INIT;
static volatile uint32_t thread_num = 1;
static lock_t logger_callback_lock = LOCK_INIT;
static void (* volatile log_callback_func)(int32_t tag_id, int debug_level, const char *message);


/*
 * Keep the thread ID and the tag ID thread local.
 */

static THREAD_LOCAL uint32_t this_thread_num = 0;
static THREAD_LOCAL int32_t tag_id = 0;


// /* only output the version once */
// static lock_t printed_version = LOCK_INIT;



int set_debug_level(int level)
{
    int old_level = global_debug_level;

    global_debug_level = level;

    return old_level;
}


int get_debug_level(void)
{
    return global_debug_level;
}



void debug_set_tag_id(int32_t t_id)
{
    tag_id = t_id;
}



static uint32_t get_thread_id()
{
    if(!this_thread_num) {
        spin_block(&thread_num_lock) {
            this_thread_num = thread_num;
            thread_num++;
        }
    }

    return this_thread_num;
}

// static int make_prefix(char *prefix_buf, int prefix_buf_size)
// {
//     struct tm t;
//     time_t epoch;
//     int64_t epoch_ms;
//     int remainder_ms;
//     int rc = PLCTAG_STATUS_OK;

//     /* make sure we have room, MAGIC */
//     if(prefix_buf_size < 37) {
//         return PLCTAG_ERR_TOO_SMALL;
//     }

//     /* build the prefix */

//     /* get the time parts */
//     epoch_ms = time_ms();
//     epoch = (time_t)(epoch_ms/1000);
//     remainder_ms = (int)(epoch_ms % 1000);

//     /* FIXME - should capture error return! */
//     localtime_r(&epoch,&t);

//     /* create the prefix and format for the file entry. */
//     rc = snprintf(prefix_buf, (size_t)prefix_buf_size,"%04d-%02d-%02d %02d:%02d:%02d.%03d thread(%u) tag(%d)",
//                   t.tm_year+1900,t.tm_mon,t.tm_mday,t.tm_hour,t.tm_min,t.tm_sec,remainder_ms, get_thread_id(), tag_id);

//     /* enforce zero string termination */
//     if(rc > 1 && rc < prefix_buf_size) {
//         prefix_buf[rc] = 0;
//     } else {
//         prefix_buf[prefix_buf_size - 1] = 0;
//     }

//     return rc;
// }


static const char *debug_level_name[DEBUG_END] = {"NONE", "ERROR", "WARN", "INFO", "DETAIL", "SPEW"};

void pdebug_impl(const char *func, int line_num, int debug_level, const char *templ, ...)
{
    va_list va;
    struct tm t;
    time_t epoch;
    int64_t epoch_ms;
    int remainder_ms;
    char prefix[1000]; /* MAGIC */
    int prefix_size = 0;
    char output[1000];
    //int output_size = 0;

    /* build the prefix */
    // prefix_size = make_prefix(prefix,(int)sizeof(prefix));  /* don't exceed a size that int can express! */
    // if(prefix_size <= 0) {
    //     return;
    // }

    /* get the time parts */
    epoch_ms = time_ms();
    epoch = (time_t)(epoch_ms/1000);
    remainder_ms = (int)(epoch_ms % 1000);

    /* FIXME - should capture error return! */
    localtime_r(&epoch,&t);

    /* print only once */
    /* FIXME - this may not be safe. */
    // if(!printed_version && debug_level >= DEBUG_INFO) {
    //     if(lock_acquire_try((lock_t*)&printed_version)) {
    //         /* create the output string template */
    //         fprintf(stderr,"%s INFO libplctag version %s, debug level %d.\n",prefix, VERSION, get_debug_level());
    //     }
    // }

    /* build the output string template */
    prefix_size += snprintf(prefix, sizeof(prefix),"%04d-%02d-%02d %02d:%02d:%02d.%03d thread(%u) tag(%" PRId32 ") %s %s:%d %s\n",
                                                    t.tm_year+1900,
                                                    t.tm_mon + 1, /* month is 0-11? */
                                                    t.tm_mday,
                                                    t.tm_hour,
                                                    t.tm_min,
                                                    t.tm_sec,
                                                    remainder_ms, 
                                                    get_thread_id(), 
                                                    tag_id, 
                                                    debug_level_name[debug_level], 
                                                    func, 
                                                    line_num, 
                                                    templ);

    /* make sure it is zero terminated */
    prefix[sizeof(prefix)-1] = 0;

    /* print it out. */
    va_start(va,templ);

    /* FIXME - check the output size */
    /*output_size = */vsnprintf(output, sizeof(output), prefix, va);
    if(log_callback_func) {
        log_callback_func(tag_id, debug_level, output);
    } else {
        fputs(output, stderr);
    }

    va_end(va);
}




#define COLUMNS (16)

void pdebug_dump_bytes_impl(const char *func, int line_num, int debug_level, uint8_t *data,int count)
{
    int max_row, row, column;
    char row_buf[(COLUMNS * 3) + 5 + 1]; 

    /* determine the number of rows we will need to print. */
    max_row = (count  + (COLUMNS - 1))/COLUMNS;

    for(row = 0; row < max_row; row++) {
        int offset = (row * COLUMNS);
        int row_offset = 0;

        /* print the offset in the packet */
        row_offset = snprintf(&row_buf[0], sizeof(row_buf),"%05d", offset);

        for(column = 0; column < COLUMNS && ((row * COLUMNS) + column) < count && row_offset < (int)sizeof(row_buf); column++) {
            offset = (row * COLUMNS) + column;
            row_offset += snprintf(&row_buf[row_offset], sizeof(row_buf) - (size_t)row_offset, " %02x", data[offset]);
        }

        /* terminate the row string*/
        row_buf[sizeof(row_buf)-1] = 0; /* just in case */

        /* output it, finally */
        pdebug_impl(func, line_num, debug_level, row_buf);
    }
}


int debug_register_logger(void (*log_callback_func_arg)(int32_t tag_id, int debug_level, const char *message))
{
    int rc = PLCTAG_STATUS_OK;

    spin_block(&logger_callback_lock) {
        if(!log_callback_func) {
            log_callback_func = log_callback_func_arg;
        } else {
            rc = PLCTAG_ERR_DUPLICATE;
        }
    }

    return rc;
}


int debug_unregister_logger(void)
{
    int rc = PLCTAG_STATUS_OK;

    spin_block(&logger_callback_lock) {
        if(log_callback_func) {
            log_callback_func = NULL;
        } else {
            rc = PLCTAG_ERR_NOT_FOUND;
        }
    }

    return rc;
}

#endif // __UTIL_DEBUG_C__


#ifndef __UTIL_HASH_C__
#define __UTIL_HASH_C__

/*
 * This comes from Bob Jenkins's excellent site:
 *    http://burtleburtle.net/bob/c/lookup3.c
 * Thanks, Bob!
 */


/*
--------------------------------------------------------------------
lookup2.c, by Bob Jenkins, December 1996, Public Domain.
hash(), hash2(), hash3, and mix() are externally useful functions.
Routines to test the hash are included if SELF_TEST is defined.
You can use this free for any purpose.  It has no warranty.

Obsolete.  Use lookup3.c instead, it is faster and more thorough.
--------------------------------------------------------------------
*/
#define SELF_TEST

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
typedef uint32_t ub4;   /* unsigned 4-byte quantities */
typedef uint8_t ub1;

#define hashsize(n) ((ub4)1<<(n))
#define hashmask(n) (hashsize(n)-1)

/*
--------------------------------------------------------------------
mix -- mix 3 32-bit values reversibly.
For every delta with one or two bit set, and the deltas of all three
  high bits or all three low bits, whether the original value of a,b,c
  is almost all zero or is uniformly distributed,
* If mix() is run forward or backward, at least 32 bits in a,b,c
  have at least 1/4 probability of changing.
* If mix() is run forward, every bit of c will change between 1/3 and
  2/3 of the time.  (Well, 22/100 and 78/100 for some 2-bit deltas.)
mix() was built out of 36 single-cycle latency instructions in a
  structure that could supported 2x parallelism, like so:
      a -= b;
      a -= c; x = (c>>13);
      b -= c; a ^= x;
      b -= a; x = (a<<8);
      c -= a; b ^= x;
      c -= b; x = (b>>13);
      ...
  Unfortunately, superscalar Pentiums and Sparcs can't take advantage
  of that parallelism.  They've also turned some of those single-cycle
  latency instructions into multi-cycle latency instructions.  Still,
  this is the fastest good hash I could find.  There were about 2^^68
  to choose from.  I only looked at a billion or so.
--------------------------------------------------------------------
*/
#define mix(a,b,c) \
    { \
        a -= b; a -= c; a ^= (c>>13); \
        b -= c; b -= a; b ^= (a<<8); \
        c -= a; c -= b; c ^= (b>>13); \
        a -= b; a -= c; a ^= (c>>12);  \
        b -= c; b -= a; b ^= (a<<16); \
        c -= a; c -= b; c ^= (b>>5); \
        a -= b; a -= c; a ^= (c>>3);  \
        b -= c; b -= a; b ^= (a<<10); \
        c -= a; c -= b; c ^= (b>>15); \
    }

/* same, but slower, works on systems that might have 8 byte ub4's */
#define mix2(a,b,c) \
    { \
        a -= b; a -= c; a ^= (c>>13); \
        b -= c; b -= a; b ^= (a<< 8); \
        c -= a; c -= b; c ^= ((b&0xffffffff)>>13); \
        a -= b; a -= c; a ^= ((c&0xffffffff)>>12); \
        b -= c; b -= a; b = (b ^ (a<<16)) & 0xffffffff; \
        c -= a; c -= b; c = (c ^ (b>> 5)) & 0xffffffff; \
        a -= b; a -= c; a = (a ^ (c>> 3)) & 0xffffffff; \
        b -= c; b -= a; b = (b ^ (a<<10)) & 0xffffffff; \
        c -= a; c -= b; c = (c ^ (b>>15)) & 0xffffffff; \
    }

/*
--------------------------------------------------------------------
hash() -- hash a variable-length key into a 32-bit value
  k     : the key (the unaligned variable-length array of bytes)
  len   : the length of the key, counting by bytes
  level : can be any 4-byte value
Returns a 32-bit value.  Every bit of the key affects every bit of
the return value.  Every 1-bit and 2-bit delta achieves avalanche.
About 36+6len instructions.

The best hash table sizes are powers of 2.  There is no need to do
mod a prime (mod is sooo slow!).  If you need less than 32 bits,
use a bitmask.  For example, if you need only 10 bits, do
  h = (h & hashmask(10));
In which case, the hash table should have hashsize(10) elements.

If you are hashing n strings (ub1 **)k, do it like this:
  for (i=0, h=0; i<n; ++i) h = hash( k[i], len[i], h);

By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.  You may use this
code any way you wish, private, educational, or commercial.  It's free.

See http://burtleburtle.net/bob/hash/evahash.html
Use for hash table lookup, or anything where one collision in 2^32 is
acceptable.  Do NOT use for cryptographic purposes.
--------------------------------------------------------------------
*/

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4061)
#endif

uint32_t hash( uint8_t *k, size_t length, uint32_t initval)
{
    uint32_t a,b,c,len;

    /* Set up the internal state */
    len = (uint32_t)length;
    a = b = 0x9e3779b9;  /* the golden ratio; an arbitrary value */
    c = initval;           /* the previous hash value */

    /*---------------------------------------- handle most of the key */
    while (len >= 12) {
        a += (uint32_t)((k[0] +((ub4)k[1]<<8) +((ub4)k[2]<<16) +((ub4)k[3]<<24)));
        b += (uint32_t)((k[4] +((ub4)k[5]<<8) +((ub4)k[6]<<16) +((ub4)k[7]<<24)));
        c += (uint32_t)((k[8] +((ub4)k[9]<<8) +((ub4)k[10]<<16)+((ub4)k[11]<<24)));
        mix(a,b,c);
        k += 12;
        len -= 12;
    }

    /*------------------------------------- handle the last 11 bytes */
    c += (uint32_t)length;
    switch(len) {            /* all the case statements fall through */
    case 11:
        c += ((ub4)k[10]<<24);
        /* Falls through. */
    case 10:
        c+=((ub4)k[9]<<16);
        /* Falls through. */
    case 9 :
        c+=((ub4)k[8]<<8);
        /* the first byte of c is reserved for the length */
        /* Falls through. */
    case 8 :
        b+=((ub4)k[7]<<24);
        /* Falls through. */
    case 7 :
        b+=((ub4)k[6]<<16);
        /* Falls through. */
    case 6 :
        b+=((ub4)k[5]<<8);
        /* Falls through. */
    case 5 :
        b+=k[4];
        /* Falls through. */
    case 4 :
        a+=((ub4)k[3]<<24);
        /* Falls through. */
    case 3 :
        a+=((ub4)k[2]<<16);
        /* Falls through. */
    case 2 :
        a+=((ub4)k[1]<<8);
        /* Falls through. */
    case 1 :
        a+=k[0];
        /* case 0: nothing left to add */
        /* Falls through. */
    }
    mix(a,b,c);
    /*-------------------------------------------- report the result */
    return c;
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // __UTIL_HASH_C__


#ifndef __UTIL_HASHTABLE_C__
#define __UTIL_HASHTABLE_C__

/*
 * This implements a simple linear probing hash table.
 *
 * Note that it will readjust its size if enough entries are made.
 */

#define MAX_ITERATIONS  (10)
#define MAX_INCREMENT (10000)

struct hashtable_entry_t {
    void *data;
    int64_t key;
};

struct hashtable_t {
    int total_entries;
    int used_entries;
    uint32_t hash_salt;
    struct hashtable_entry_t *entries;
};


typedef struct hashtable_entry_t *hashtable_entry_p;

//static int next_highest_prime(int x);
static int find_key(hashtable_p table, int64_t key);
static int find_empty(hashtable_p table, int64_t key);
static int expand_table(hashtable_p table);


hashtable_p hashtable_create(int initial_capacity)
{
    hashtable_p tab = NULL;

    pdebug(DEBUG_INFO,"Starting");

    if(initial_capacity <= 0) {
        pdebug(DEBUG_WARN,"Size is less than or equal to zero!");
        return NULL;
    }

    tab = (hashtable_p)mem_alloc(sizeof(struct hashtable_t));
    if(!tab) {
        pdebug(DEBUG_ERROR,"Unable to allocate memory for hash table!");
        return NULL;
    }

    tab->total_entries = initial_capacity;
    tab->used_entries = 0;
    tab->hash_salt = (uint32_t)(time_ms()) + (uint32_t)(intptr_t)(tab);

    tab->entries = (hashtable_entry_t*)mem_alloc(initial_capacity * (int)sizeof(struct hashtable_entry_t));
    if(!tab->entries) {
        pdebug(DEBUG_ERROR,"Unable to allocate entry array!");
        hashtable_destroy(tab);
        return NULL;
    }

    pdebug(DEBUG_INFO,"Done");

    return tab;
}


void *hashtable_get(hashtable_p table, int64_t key)
{
    int index = 0;
    void *result = NULL;

    pdebug(DEBUG_SPEW,"Starting");

    if(!table) {
        pdebug(DEBUG_WARN,"Hashtable pointer null or invalid.");
        return NULL;
    }

    index = find_key(table, key);
    if(index != PLCTAG_ERR_NOT_FOUND) {
        result = table->entries[index].data;
        pdebug(DEBUG_SPEW,"found data %p", result);
    } else {
        pdebug(DEBUG_SPEW, "key not found!");
    }

    pdebug(DEBUG_SPEW,"Done");

    return result;
}


int hashtable_put(hashtable_p table, int64_t key, void  *data)
{
    int rc = PLCTAG_STATUS_OK;
    int index = 0;

    pdebug(DEBUG_SPEW,"Starting");

    if(!table) {
        pdebug(DEBUG_WARN,"Hashtable pointer null or invalid.");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* try to find a slot to put the new entry */
    index = find_empty(table, key);
    while(index == PLCTAG_ERR_NOT_FOUND) {
        rc = expand_table(table);
        if(rc != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Unable to expand table to make entry unique!");
            return rc;
        }

        index = find_empty(table, key);
    }

    pdebug(DEBUG_SPEW, "Putting value at index %d", index);

    table->entries[index].key = key;
    table->entries[index].data = data;
    table->used_entries++;

    pdebug(DEBUG_SPEW, "Done.");

    return PLCTAG_STATUS_OK;
}


void *hashtable_get_index(hashtable_p table, int index)
{
    if(!table) {
        pdebug(DEBUG_WARN,"Hashtable pointer null or invalid");
        return NULL;
    }

    if(index < 0 || index >= table->total_entries) {
        pdebug(DEBUG_WARN, "Out of bounds index!");
        return NULL;
    }

    return table->entries[index].data;
}



int hashtable_capacity(hashtable_p table)
{
    if(!table) {
        pdebug(DEBUG_WARN,"Hashtable pointer null or invalid");
        return PLCTAG_ERR_NULL_PTR;
    }

    return table->total_entries;
}



int hashtable_entries(hashtable_p table)
{
    if(!table) {
        pdebug(DEBUG_WARN,"Hashtable pointer null or invalid");
        return PLCTAG_ERR_NULL_PTR;
    }

    return table->used_entries;
}



int hashtable_on_each(hashtable_p table, int (*callback_func)(hashtable_p table, int64_t key, void *data, void *context), void *context_arg)
{
    int rc = PLCTAG_STATUS_OK;

    if(!table) {
        pdebug(DEBUG_WARN,"Hashtable pointer null or invalid");
    }

    for(int i=0; i < table->total_entries && rc == PLCTAG_STATUS_OK; i++) {
        if(table->entries[i].data) {
            rc = callback_func(table, table->entries[i].key, table->entries[i].data, context_arg);
        }
    }

    return rc;
}



void *hashtable_remove(hashtable_p table, int64_t key)
{
    int index = 0;
    void *result = NULL;

    pdebug(DEBUG_DETAIL,"Starting");

    if(!table) {
        pdebug(DEBUG_WARN,"Hashtable pointer null or invalid.");
        return result;
    }

    index = find_key(table, key);
    if(index == PLCTAG_ERR_NOT_FOUND) {
        pdebug(DEBUG_SPEW,"Not found.");
        return result;
    }

    result = table->entries[index].data;
    table->entries[index].key = 0;
    table->entries[index].data = NULL;
    table->used_entries--;

    pdebug(DEBUG_DETAIL,"Done");

    return result;
}




int hashtable_destroy(hashtable_p table)
{
    pdebug(DEBUG_INFO,"Starting");

    if(!table) {
        pdebug(DEBUG_WARN,"Called with null pointer!");
        return PLCTAG_ERR_NULL_PTR;
    }

    mem_free(table->entries);
    table->entries = NULL;

    mem_free(table);

    pdebug(DEBUG_INFO,"Done");

    return PLCTAG_STATUS_OK;
}





/***********************************************************************
 *************************** Helper Functions **************************
 **********************************************************************/


#define KEY_TO_INDEX(t, k) (uint32_t)((hash((uint8_t*)&k, sizeof(k), t->hash_salt)) % (uint32_t)(t->total_entries))


int find_key(hashtable_p table, int64_t key)
{
    uint32_t initial_index = KEY_TO_INDEX(table, key);
    int index = 0;
    int iteration = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    /*
     * search for the hash value.
     *
     * Note: do NOT stop if we find a NULL entry.  That might be the result
     * of a removed entry and there could still be entries past that point
     * that are for the initial slot.
     */
    for(iteration=0; iteration < MAX_ITERATIONS; iteration++) {
        index = ((int)initial_index + iteration) % table->total_entries;

        if(table->entries[index].key == key) {
            break;
        }
    }

    if(iteration >= MAX_ITERATIONS) {
        /* FIXME - does not work on Windows. */
        //pdebug(DEBUG_SPEW, "Key %ld not found.", key);
        return PLCTAG_ERR_NOT_FOUND;
    } else {
        //pdebug(DEBUG_SPEW, "Key %d found at index %d.", (int)table->entries[index].key, index);
    }

    pdebug(DEBUG_SPEW, "Done.");

    return index;
}




int find_empty(hashtable_p table, int64_t key)
{
    uint32_t initial_index = KEY_TO_INDEX(table, key);
    int index = 0;
    int iteration = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    /* search for the hash value. */
    for(iteration=0; iteration < MAX_ITERATIONS; iteration++) {
        index = ((int)initial_index + iteration) % table->total_entries;

        pdebug(DEBUG_SPEW, "Trying index %d for key %ld.", index, key);
        if(table->entries[index].data == NULL) {
            break;
        }
    }

    if(iteration >= MAX_ITERATIONS) {
        pdebug(DEBUG_SPEW,"No empty entry found in %d iterations!", MAX_ITERATIONS);
        return PLCTAG_ERR_NOT_FOUND;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return index;
}




int expand_table(hashtable_p table)
{
    struct hashtable_t new_table;
    int total_entries = table->total_entries;
    int index = PLCTAG_ERR_NOT_FOUND;

    pdebug(DEBUG_SPEW, "Starting.");

    pdebug(DEBUG_SPEW, "Table using %d entries of %d.", table->used_entries, table->total_entries);

    do {
        /* double entries unless already at max doubling, then increment. */
        total_entries += (total_entries < MAX_INCREMENT ? total_entries : MAX_INCREMENT);

        new_table.total_entries = total_entries;
        new_table.used_entries = 0;
        new_table.hash_salt = table->hash_salt;

        pdebug(DEBUG_SPEW, "trying new size = %d", total_entries);

        new_table.entries = (hashtable_entry_t*)mem_alloc(total_entries * (int)sizeof(struct hashtable_entry_t));
        if(!new_table.entries) {
            pdebug(DEBUG_ERROR, "Unable to allocate new entry array!");
            return PLCTAG_ERR_NO_MEM;
        }

        /* copy the old entries.  Only copy ones that are used. */
        for(int i=0; i < table->total_entries; i++) {
            if(table->entries[i].data) {
                index = find_empty(&new_table, table->entries[i].key);
                if(index == PLCTAG_ERR_NOT_FOUND) {
                    /* oops, still cannot insert all the entries! Try again. */
                    pdebug(DEBUG_DETAIL, "Unable to insert existing entry into expanded table. Retrying.");
                    mem_free(new_table.entries);
                    break;
                } else {
                    /* store the entry into the new table. */
                    new_table.entries[index] = table->entries[i];
                    new_table.used_entries++;
                }
            }
        }
    } while(index == PLCTAG_ERR_NOT_FOUND);

    /* success! */
    mem_free(table->entries);
    table->entries = new_table.entries;
    table->total_entries = new_table.total_entries;
    table->used_entries = new_table.used_entries;

    pdebug(DEBUG_SPEW, "Done.");

    return PLCTAG_STATUS_OK;
}

#endif // __UTIL_HASHTABLE_C__


#ifndef __UTIL_RC_C__
#define __UTIL_RC_C__


//~ #ifndef container_of
//~ #define container_of(ptr, type, member) ((type *)((char *)(1 ? (ptr) : &((type *)0)->member) - offsetof(type, member)))
//~ #endif




/*
 * Handle clean up functions.
 */

//typedef struct cleanup_t *cleanup_p;
//
//struct cleanup_t {
//    cleanup_p next;
//    const char *function_name;
//    int line_num;
//    int extra_arg_count;
//    void **extra_args;
//    rc_cleanup_func cleanup_func;
//    void *dummy[]; /* force alignment */
//};


/*
 * This is a rc struct that we use to make sure that we are able to align
 * the remaining part of the allocated block.
 */

struct refcount_t {
    lock_t lock;
    int count;
    const char *function_name;
    int line_num;
    //cleanup_p cleaners;
    rc_cleanup_func cleanup_func;

    /* FIXME - needed for alignment, this is a hack! */
    union {
        uint8_t dummy_u8;
        uint16_t dummy_u16;
        uint32_t dummy_u32;
        uint64_t dummy_u64;
        double dummy_double;
        void *dummy_ptr;
        void (*dummy_func)(void);
    } dummy_align[];
};


typedef struct refcount_t *refcount_p;

static void refcount_cleanup(refcount_p rc);
//static cleanup_p cleanup_entry_create(const char *func, int line_num, rc_cleanup_func cleaner, int extra_arg_count, va_list extra_args);
//static void cleanup_entry_destroy(cleanup_p entry);



/*
 * rc_alloc
 *
 * Create a reference counted control for the requested data size.  Return a strong
 * reference to the data.
 */
//void *rc_alloc_impl(const char *func, int line_num, int data_size, int extra_arg_count, rc_cleanup_func cleaner_func, ...)
void *rc_alloc_impl(const char *func, int line_num, int data_size, rc_cleanup_func cleaner_func)
{
    refcount_p rc = NULL;
    //cleanup_p cleanup = NULL;
    //va_list extra_args;

    pdebug(DEBUG_INFO,"Starting, called from %s:%d",func, line_num);

    pdebug(DEBUG_SPEW,"Allocating %d-byte refcount struct",(int)sizeof(struct refcount_t));

    rc = (refcount_p)mem_alloc((int)sizeof(struct refcount_t) + data_size);
    if(!rc) {
        pdebug(DEBUG_WARN,"Unable to allocate refcount struct!");
        return NULL;
    }

    rc->count = 1;  /* start with a reference count. */
    rc->lock = LOCK_INIT;

    rc->cleanup_func = cleaner_func;

    /* store where we were called from for later. */
    rc->function_name = func;
    rc->line_num = line_num;

    pdebug(DEBUG_INFO, "Done");

    /* return the original address if successful otherwise NULL. */

    /* DEBUG */
    pdebug(DEBUG_DETAIL,"Returning memory pointer %p",(char *)(rc + 1));

    return (char *)(rc + 1);
}








/*
 * Increments the ref count if the reference is valid.
 *
 * It returns the original poiner if the passed pointer was valid.  It returns
 * NULL if the passed pointer was invalid.
 *
 * This is for usage like:
 * my_struct->some_field_ref = rc_inc(ref);
 */

void *rc_inc_impl(const char *func, int line_num, void *data)
{
    int count = 0;
    refcount_p rc = NULL;
    char *result = NULL;

    pdebug(DEBUG_SPEW,"Starting, called from %s:%d for %p",func, line_num, data);

    if(!data) {
        pdebug(DEBUG_SPEW,"Invalid pointer passed from %s:%d!", func, line_num);
        return result;
    }

    /* get the refcount structure. */
    rc = ((refcount_p)data) - 1;

    /* spin until we have ownership */
    spin_block(&rc->lock) {
        if(rc->count > 0) {
            rc->count++;
            count = rc->count;
            result = (char*)data;
        } else {
            count = rc->count;
            result = NULL;
        }
    }

    if(!result) {
        pdebug(DEBUG_SPEW,"Invalid ref count (%d) from call at %s line %d!  Unable to take strong reference.", count, func, line_num);
    } else {
        pdebug(DEBUG_SPEW,"Ref count is %d for %p.", count, data);
    }

    /* return the result pointer. */
    return result;
}



/*
 * Decrement the ref count.
 *
 * This is for usage like:
 * my_struct->some_field = rc_dec(rc_obj);
 *
 * Note that the final clean up function _MUST_ free the data pointer
 * passed to it.   It must clean up anything referenced by that data,
 * and the block itself using mem_free() or the appropriate function;
 */

void *rc_dec_impl(const char *func, int line_num, void *data)
{
    int count = 0;
    int invalid = 0;
    refcount_p rc = NULL;

    pdebug(DEBUG_SPEW,"Starting, called from %s:%d for %p",func, line_num, data);

    if(!data) {
        pdebug(DEBUG_SPEW,"Null reference passed from %s:%d!", func, line_num);
        return NULL;
    }

    /* get the refcount structure. */
    rc = ((refcount_p)data) - 1;

    /* do this sorta atomically */
    spin_block(&rc->lock) {
        if(rc->count > 0) {
            rc->count--;
            count = rc->count;
        } else {
            count = rc->count;
            invalid = 1;
        }
    }

    if(invalid) {
        pdebug(DEBUG_WARN,"Reference has invalid count %d!", count);
    } else {
        pdebug(DEBUG_SPEW,"Ref count is %d for %p.", count, data);

        /* clean up only if count is zero. */
        if(rc && count <= 0) {
            pdebug(DEBUG_DETAIL,"Calling cleanup functions due to call at %s:%d for %p.", func, line_num, data);

            refcount_cleanup(rc);
        }
    }

    return NULL;
}




void refcount_cleanup(refcount_p rc)
{
    pdebug(DEBUG_INFO,"Starting");
    if(!rc) {
        pdebug(DEBUG_WARN,"Refcount is NULL!");
        return;
    }

    /* call the clean up function */
    rc->cleanup_func((void *)(rc+1));

    /* finally done. */
    mem_free(rc);

    pdebug(DEBUG_INFO,"Done.");
}

#endif // __UTIL_RC_C__


#ifndef __UTIL_VECTOR_C__
#define __UTIL_VECTOR_C__

struct vector_t {
    int len;
    int capacity;
    int max_inc;
    void **data;
};



static int ensure_capacity(vector_p vec, int capacity);


vector_p vector_create(int capacity, int max_inc)
{
    vector_p vec = NULL;

    pdebug(DEBUG_SPEW,"Starting");

    if(capacity <= 0) {
        pdebug(DEBUG_WARN, "Called with negative capacity!");
        return NULL;
    }

    if(max_inc <= 0) {
        pdebug(DEBUG_WARN, "Called with negative maximum size increment!");
        return NULL;
    }

    vec = (vector_p)mem_alloc((int)sizeof(struct vector_t));
    if(!vec) {
        pdebug(DEBUG_ERROR,"Unable to allocate memory for vector!");
        return NULL;
    }

    vec->len = 0;
    vec->capacity = capacity;
    vec->max_inc = max_inc;

    vec->data = (void**)mem_alloc(capacity * (int)sizeof(void *));
    if(!vec->data) {
        pdebug(DEBUG_ERROR,"Unable to allocate memory for vector data!");
        vector_destroy(vec);
        return NULL;
    }

    pdebug(DEBUG_SPEW,"Done");

    return vec;
}



int vector_length(vector_p vec)
{
    pdebug(DEBUG_SPEW,"Starting");

    /* check to see if the vector ref is valid */
    if(!vec) {
        pdebug(DEBUG_WARN,"Null pointer or invalid pointer to vector passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    pdebug(DEBUG_SPEW,"Done");

    return vec->len;
}



int vector_put(vector_p vec, int index, void * data)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW,"Starting");

    /* check to see if the vector ref is valid */
    if(!vec) {
       pdebug(DEBUG_WARN,"Null pointer or invalid pointer to vector passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(index < 0) {
        pdebug(DEBUG_WARN,"Index is negative!");
        return PLCTAG_ERR_OUT_OF_BOUNDS;
    }

    rc = ensure_capacity(vec, index+1);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to ensure capacity!");
        return rc;
    }

    /* reference the new data. */
    vec->data[index] = data;

    /* adjust the length, if needed */
    if(index >= vec->len) {
        vec->len = index+1;
    }

    pdebug(DEBUG_SPEW,"Done");

    return rc;
}


void * vector_get(vector_p vec, int index)
{
    pdebug(DEBUG_SPEW,"Starting");

    /* check to see if the vector ref is valid */
    if(!vec) {
        pdebug(DEBUG_WARN,"Null pointer or invalid pointer to vector passed!");
        return NULL;
    }

    if(index < 0 || index >= vec->len) {
        pdebug(DEBUG_WARN,"Index is out of bounds!");
        return NULL;
    }

    pdebug(DEBUG_SPEW,"Done");

    return vec->data[index];
}


void * vector_remove(vector_p vec, int index)
{
    void * result = NULL;

    pdebug(DEBUG_SPEW,"Starting");

    /* check to see if the vector ref is valid */
    if(!vec) {
        pdebug(DEBUG_WARN,"Null pointer or invalid pointer to vector passed!");
        return NULL;
    }

    if(index < 0 || index >= vec->len) {
        pdebug(DEBUG_WARN,"Index is out of bounds!");
        return NULL;
    }

    /* get the value in this slot before we overwrite it. */
    result = vec->data[index];

    /* move the rest of the data over this. */
    mem_move(&vec->data[index], &vec->data[index+1], (int)((sizeof(void *) * (size_t)(vec->len - index - 1))));

    /* make sure that we do not have old data hanging around. */
    vec->data[vec->len - 1] = NULL;

    /* adjust the length to the new size */
    vec->len--;

    pdebug(DEBUG_SPEW,"Done");

    return result;
}



int vector_destroy(vector_p vec)
{
    pdebug(DEBUG_SPEW,"Starting.");

    if(!vec) {
        pdebug(DEBUG_WARN,"Null pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    mem_free(vec->data);
    mem_free(vec);

    pdebug(DEBUG_SPEW,"Done.");

    return PLCTAG_STATUS_OK;
}



/***********************************************************************
 *************** Private Helper Functions ******************************
 **********************************************************************/



int ensure_capacity(vector_p vec, int capacity)
{
    int new_inc = 0;
    void * *new_data = NULL;

    if(!vec) {
        pdebug(DEBUG_WARN,"Null pointer or invalid pointer to vector passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* is there anything to do? */
    if(capacity <= vec->capacity) {
        /* release the reference */
        return PLCTAG_STATUS_OK;
    }

    /* calculate the new capacity
     *
     * Start by guessing 50% larger.  Clamp that against 1 at the
     * low end and the max increment passed when the vector was created.
     */
    new_inc = vec->capacity / 2;

    if(new_inc > vec->max_inc) {
        new_inc = vec->max_inc;
    }

    if(new_inc < 1) {
        new_inc = 1;
    }

    /* allocate the new data area */
    new_data = (void * *)mem_alloc((int)((sizeof(void *) * (size_t)(vec->capacity + new_inc))));
    if(!new_data) {
        pdebug(DEBUG_ERROR,"Unable to allocate new data area!");
        return PLCTAG_ERR_NO_MEM;
    }

    mem_copy(new_data, vec->data, (int)((size_t)(vec->capacity) * sizeof(void *)));

    mem_free(vec->data);

    vec->data = new_data;

    vec->capacity += new_inc;

    return PLCTAG_STATUS_OK;
}

#endif // __UTIL_VECTOR_C__


#ifndef __PROTOCOLS_MB_MODBUS_C__
#define __PROTOCOLS_MB_MODBUS_C__

#include <ctype.h>
#include <float.h>
#include <inttypes.h>
#include <limits.h>
#include <stdlib.h>

/* data definitions */

#define PLC_SOCKET_ERR_MAX_DELAY (5000)
#define PLC_SOCKET_ERR_START_DELAY (50)
#define PLC_SOCKET_ERR_DELAY_WAIT_INCREMENT (10)
#define MODBUS_DEFAULT_PORT (502)
#define PLC_READ_DATA_LEN (300)
#define PLC_WRITE_DATA_LEN (300)
#define MODBUS_MBAP_SIZE (6)
#define MAX_MODBUS_REQUEST_PAYLOAD (246)
#define MAX_MODBUS_RESPONSE_PAYLOAD (250)
#define MAX_MODBUS_PDU_PAYLOAD (253)  /* everything after the server address */
#define MODBUS_INACTIVITY_TIMEOUT (5000)
#define SOCKET_READ_TIMEOUT (20) /* read timeout in milliseconds */
#define SOCKET_WRITE_TIMEOUT (20) /* write timeout in milliseconds */
#define SOCKET_CONNECT_TIMEOUT (20) /* connect timeout step in milliseconds */
#define MODBUS_IDLE_WAIT_TIMEOUT (100) /* idle wait timeout in milliseconds */
#define MAX_MODBUS_REQUESTS (16) /* per the Modbus specification */

typedef struct modbus_tag_t *modbus_tag_p;
typedef struct modbus_tag_list_t *modbus_tag_list_p;

struct modbus_tag_list_t {
    struct modbus_tag_t *head;
    struct modbus_tag_t *tail;
};



struct modbus_plc_t {
    struct modbus_plc_t *next;

    /* keep a list of tags for this PLC. */
    struct modbus_tag_list_t tag_list;
    // struct modbus_tag_list_t request_tag_list;
    // struct modbus_tag_list_t response_tag_list;

    /* hostname/ip and possibly port of the server. */
    char *server;
    sock_p sock;
    uint8_t server_id;
    int connection_group_id;

    /* State */
    struct {
        unsigned int terminate:1;
        unsigned int response_ready:1;
        unsigned int request_ready:1;
        // unsigned int request_in_flight:1;
    } flags;
    uint16_t seq_id;

    /* thread related state */
    thread_p handler_thread;
    mutex_p mutex;
    //cond_p wait_cond;
    enum {
        PLC_CONNECT_START = 0,
        PLC_CONNECT_WAIT,
        PLC_READY,
        PLC_BUILD_REQUEST,
        PLC_SEND_REQUEST,
        PLC_RECEIVE_RESPONSE,
        PLC_ERR_WAIT
    } state;
    int max_requests_in_flight;
    int32_t tags_with_requests[MAX_MODBUS_REQUESTS];

    /* comms timeout/disconnect. */
    int64_t inactivity_timeout_ms;

    /* data */
    int read_data_len;
    uint8_t read_data[PLC_READ_DATA_LEN];
    int32_t response_tag_id;

    int write_data_len;
    int write_data_offset;
    uint8_t write_data[PLC_WRITE_DATA_LEN];
    int32_t request_tag_id;
};

typedef struct modbus_plc_t *modbus_plc_p;

typedef enum { MB_REG_UNKNOWN, MB_REG_COIL, MB_REG_DISCRETE_INPUT, MB_REG_HOLDING_REGISTER, MB_REG_INPUT_REGISTER } modbus_reg_type_t;

typedef enum {
    MB_CMD_READ_COIL_MULTI = 0x01,
    MB_CMD_READ_DISCRETE_INPUT_MULTI = 0x02,
    MB_CMD_READ_HOLDING_REGISTER_MULTI = 0x03,
    MB_CMD_READ_INPUT_REGISTER_MULTI = 0x04,
    MB_CMD_WRITE_COIL_SINGLE = 0x05,
    MB_CMD_WRITE_HOLDING_REGISTER_SINGLE = 0x06,
    MB_CMD_WRITE_COIL_MULTI = 0x0F,
    MB_CMD_WRITE_HOLDING_REGISTER_MULTI = 0x10
} modbug_cmd_t;



typedef enum {
    TAG_OP_IDLE = 0,
    TAG_OP_READ_REQUEST,
    TAG_OP_READ_RESPONSE,
    TAG_OP_WRITE_REQUEST,
    TAG_OP_WRITE_RESPONSE
} tag_op_type_t;


struct modbus_tag_t {
    /* base tag parts. */
    TAG_BASE_STRUCT;

    /* next one in the list for this PLC */
    struct modbus_tag_t *next;

    /* register type. */
    modbus_reg_type_t reg_type;
    uint16_t reg_base;

    /* the PLC we are using */
    modbus_plc_p plc;

    tag_op_type_t op;
    uint16_t request_num;
    uint16_t seq_id;

    /* which request slot are we using? */
    int request_slot;

    /* data for the tag. */
    int elem_count;
    int elem_size;
};


/* default string types used for Modbus PLCs. */
tag_byte_order_t modbus_tag_byte_order = {
    .is_allocated = 0,

    .str_is_defined = 0, /* FIXME */
    .str_is_counted = 0,
    .str_is_fixed_length = 0,
    .str_is_zero_terminated = 0,
    .str_is_byte_swapped = 0,

    .str_count_word_bytes = 0,
    .str_max_capacity = 0,
    .str_total_length = 0,
    .str_pad_bytes = 0,

    .int16_order = {1,0},
    .int32_order = {3,2,1,0},
    .int64_order = {7,6,5,4,3,2,1,0},
    .float32_order = {3,2,1,0},
    .float64_order = {7,6,5,4,3,2,1,0},    
};


/* Modbus module globals. */
mutex_p mb_mutex = NULL;
modbus_plc_p plcs = NULL;
volatile int mb_library_terminating = 0;


/* helper functions */
static int create_tag_object(attr attribs, modbus_tag_p *tag);
static int find_or_create_plc(attr attribs, modbus_plc_p *plc);
static int parse_register_name(attr attribs, modbus_reg_type_t *reg_type, int *reg_base);
static void modbus_tag_destructor(void *tag_arg);
static void modbus_plc_destructor(void *plc_arg);
static THREAD_FUNC(modbus_plc_handler);
static void wake_plc_thread(modbus_plc_p plc);
static int connect_plc(modbus_plc_p plc);
static int tickle_all_tags(modbus_plc_p plc);
static int tickle_tag(modbus_plc_p plc, modbus_tag_p tag);
static int find_request_slot(modbus_plc_p plc, modbus_tag_p tag);
static void clear_request_slot(modbus_plc_p plc, modbus_tag_p tag);
static int receive_response(modbus_plc_p plc);
static int send_request(modbus_plc_p plc);
static int check_read_response(modbus_plc_p plc, modbus_tag_p tag);
static int create_read_request(modbus_plc_p plc, modbus_tag_p tag);
static int check_write_response(modbus_plc_p plc, modbus_tag_p tag);
static int create_write_request(modbus_plc_p plc, modbus_tag_p tag);
static int translate_modbus_error(uint8_t err_code);

/* tag list functions */
// static int list_is_empty(modbus_tag_list_p list);
static modbus_tag_p pop_tag(modbus_tag_list_p list);
static void push_tag(modbus_tag_list_p list, modbus_tag_p tag);
static struct modbus_tag_list_t merge_lists(modbus_tag_list_p first, modbus_tag_list_p last);
static int remove_tag(modbus_tag_list_p list, modbus_tag_p tag);


/* tag vtable functions. */

/* control functions. */
static int mb_abort(plc_tag_p p_tag);
static int mb_read_start(plc_tag_p p_tag);
static int mb_tag_status(plc_tag_p p_tag);
static int mb_tickler(plc_tag_p p_tag);
static int mb_write_start(plc_tag_p p_tag);
static int mb_wake_plc(plc_tag_p p_tag);


/* data accessors */
static int mb_get_int_attrib(plc_tag_p tag, const char *attrib_name, int default_value);
static int mb_set_int_attrib(plc_tag_p tag, const char *attrib_name, int new_value);

struct tag_vtable_t modbus_vtable = {
    (tag_vtable_func)mb_abort,
    (tag_vtable_func)mb_read_start,
    (tag_vtable_func)mb_tag_status,
    (tag_vtable_func)mb_tickler,
    (tag_vtable_func)mb_write_start,
    (tag_vtable_func)mb_wake_plc,

    /* data accessors */
    mb_get_int_attrib,
    mb_set_int_attrib
};


/****** main entry point *******/

plc_tag_p mb_tag_create(attr attribs, void (*tag_callback_func)(int32_t tag_id, int event, int status, void *userdata), void *userdata)
{
    int rc = PLCTAG_STATUS_OK;
    modbus_tag_p tag = NULL;

    pdebug(DEBUG_INFO, "Starting.");

    /* create the tag object. */
    rc = create_tag_object(attribs, &tag);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to create new tag!  Error %s!", plc_tag_decode_error(rc));
        return NULL;
    }

    /* set up the generic parts. */
    rc = plc_tag_generic_init_tag((plc_tag_p)tag, attribs, tag_callback_func, userdata);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to initialize generic tag parts!");
        rc_dec(tag);
        return (plc_tag_p)NULL;
    }

    /* find the PLC object. */
    rc = find_or_create_plc(attribs, &(tag->plc));
    if(rc == PLCTAG_STATUS_OK) {
        /* put the tag on the PLC's list. */
        critical_block(tag->plc->mutex) {
            push_tag(&(tag->plc->tag_list), tag);
        }
    } else {
        pdebug(DEBUG_WARN, "Unable to create new tag!  Error %s!", plc_tag_decode_error(rc));
        tag->status = (int8_t)rc;
    }

    /* kick off a read. */
    mb_read_start((plc_tag_p)tag);

    pdebug(DEBUG_INFO, "Done.");

    return (plc_tag_p)tag;
}




/***** helper functions *****/

int create_tag_object(attr attribs, modbus_tag_p *tag)
{
    int rc = PLCTAG_STATUS_OK;
    int data_size = 0;
    int reg_size = 0;
    int elem_count = attr_get_int(attribs, "elem_count", 1);
    modbus_reg_type_t reg_type = MB_REG_UNKNOWN;
    int reg_base = 0;

    if (elem_count < 0) {
        pdebug(DEBUG_WARN, "Element count should not be a negative value!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    pdebug(DEBUG_INFO, "Starting.");

    *tag = NULL;

    /* get register type. */
    rc = parse_register_name(attribs, &reg_type, &reg_base);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Error parsing base register name!");
        return rc;
    }

    /* determine register type. */
    switch(reg_type) {
        case MB_REG_COIL:
            /* fall through */
        case MB_REG_DISCRETE_INPUT:
            reg_size = 1;
            break;

        case MB_REG_HOLDING_REGISTER:
            /* fall through */
        case MB_REG_INPUT_REGISTER:
            reg_size = 16;
            break;

        default:
            pdebug(DEBUG_WARN, "Unsupported register type!");
            reg_size = 0;
            return PLCTAG_ERR_BAD_PARAM;
            break;
    }

    /* calculate the data size in bytes. */
    data_size = ((elem_count * reg_size) + 7) / 8;

    pdebug(DEBUG_DETAIL, "Tag data size is %d bytes.", data_size);

    /* allocate the tag */
    *tag = (modbus_tag_p)rc_alloc((int)(unsigned int)sizeof(struct modbus_tag_t)+data_size, modbus_tag_destructor);
    if(! *tag) {
        pdebug(DEBUG_WARN, "Unable to allocate Modbus tag!");
        return PLCTAG_ERR_NO_MEM;
    }

    /* point the data just after the tag struct. */
    (*tag)->data = (uint8_t *)((*tag) + 1);

    /* set the various size/element fields. */
    (*tag)->reg_base = (uint16_t)(unsigned int)reg_base;
    (*tag)->reg_type = reg_type;
    (*tag)->elem_count = elem_count;
    (*tag)->elem_size = reg_size;
    (*tag)->size = data_size;

    /* set up the vtable */
    (*tag)->vtable = &modbus_vtable;

    /* set the default byte order */
    (*tag)->byte_order = &modbus_tag_byte_order;

    /* set initial tag operation state. */
    (*tag)->op = TAG_OP_IDLE;

    /* initialize the current request slot */
    (*tag)->request_slot = -1;

    /* make sure the generic tag tickler thread does not call the generic tickler. */
    (*tag)->skip_tickler = 1;

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_OK;
}




void modbus_tag_destructor(void *tag_arg)
{
    modbus_tag_p tag = (modbus_tag_p)tag_arg;

    pdebug(DEBUG_INFO, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN, "Destructor called with null pointer!");
        return;
    }

    /* abort everything. */
    mb_abort((plc_tag_p)tag);

    if(tag->plc) {
        /* unlink the tag from the PLC. */
        critical_block(tag->plc->mutex) {
            int rc = remove_tag(&(tag->plc->tag_list), tag);
            if(rc == PLCTAG_STATUS_OK) {
                pdebug(DEBUG_DETAIL, "Tag removed from the PLC successfully.");
            } else if(rc == PLCTAG_ERR_NOT_FOUND) {
                pdebug(DEBUG_WARN, "Tag not found in the PLC's list for the tag's operation %d.", tag->op);
            } else {
                pdebug(DEBUG_WARN, "Error %s while trying to remove the tag from the PLC's list!", plc_tag_decode_error(rc));
            }
        }

        pdebug(DEBUG_DETAIL, "Releasing the reference to the PLC.");
        tag->plc = (modbus_plc_p)rc_dec(tag->plc);
    }

    if(tag->api_mutex) {
        mutex_destroy(&(tag->api_mutex));
        tag->api_mutex = NULL;
    }

    if(tag->ext_mutex) {
        mutex_destroy(&(tag->ext_mutex));
        tag->ext_mutex = NULL;
    }

    if(tag->tag_cond_wait) {
        cond_destroy(&(tag->tag_cond_wait));
        tag->tag_cond_wait = NULL;
    }

    if(tag->byte_order && tag->byte_order->is_allocated) {
        mem_free(tag->byte_order);
        tag->byte_order = NULL;
    }

    pdebug(DEBUG_INFO, "Done.");
}




int find_or_create_plc(attr attribs, modbus_plc_p *plc)
{
    const char *server = attr_get_str(attribs, "gateway", NULL);
    int server_id = attr_get_int(attribs, "path", -1);
    int connection_group_id = attr_get_int(attribs, "connection_group_id", 0);
    int max_requests_in_flight = attr_get_int(attribs, "max_requests_in_flight", 1);
    int is_new = 0;
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    /* clamp maximum requests in flight. */
    if(max_requests_in_flight > MAX_MODBUS_REQUESTS) {
        pdebug(DEBUG_WARN, "max_requests_in_flight set to %d which is higher than the Modbus limit of %d.", max_requests_in_flight, MAX_MODBUS_REQUESTS);
        max_requests_in_flight = MAX_MODBUS_REQUESTS;
    }

    if(max_requests_in_flight < 1) {
        pdebug(DEBUG_WARN, "max_requests_in_flight must be between 1 and %d, inclusive, was %d.", MAX_MODBUS_REQUESTS, max_requests_in_flight);
        max_requests_in_flight = 1;
    }

    if(server_id < 0 || server_id > 255) {
        pdebug(DEBUG_WARN, "Server ID, %d, out of bounds or missing!", server_id);
        return PLCTAG_ERR_OUT_OF_BOUNDS;
    }

    /* see if we can find a matching server. */
    critical_block(mb_mutex) {
        modbus_plc_p *walker = &plcs;

        while(*walker && (*walker)->connection_group_id != connection_group_id && (*walker)->server_id != (uint8_t)(unsigned int)server_id && str_cmp_i(server, (*walker)->server) != 0) {
            walker = &((*walker)->next);
        }

        /* did we find one. */
        if(*walker && (*walker)->connection_group_id == connection_group_id && (*walker)->server_id == (uint8_t)(unsigned int)server_id && str_cmp_i(server, (*walker)->server) == 0) {
            pdebug(DEBUG_DETAIL, "Using existing PLC connection.");
            *plc = (modbus_plc_p)rc_inc(*walker);
            is_new = 0;
        } else {
            /* nope, make a new one.  Do as little as possible in the mutex. */

            pdebug(DEBUG_DETAIL, "Creating new PLC connection.");

            is_new = 1;

            *plc = (modbus_plc_p)rc_alloc((int)(unsigned int)sizeof(struct modbus_plc_t), modbus_plc_destructor);
            if(*plc) {
                pdebug(DEBUG_DETAIL, "Setting connection_group_id to %d.", connection_group_id);
                (*plc)->connection_group_id = connection_group_id;

                /* copy the server string so that we can find this again. */
                (*plc)->server = str_dup(server);
                if(! ((*plc)->server)) {
                    pdebug(DEBUG_WARN, "Unable to allocate Modbus PLC server string!");
                    rc = PLCTAG_ERR_NO_MEM;
                } else {
                    /* make sure we can be found. */
                    (*plc)->server_id = (uint8_t)(unsigned int)server_id;

                    /* other tags could try to add themselves immediately. */

                    /* clear tag list */
                    (*plc)->tag_list.head = NULL;
                    (*plc)->tag_list.tail = NULL;

                    /* create the PLC mutex to protect the tag list. */
                    rc = mutex_create(&((*plc)->mutex));
                    if(rc != PLCTAG_STATUS_OK) {
                        pdebug(DEBUG_WARN, "Unable to create new mutex, error %s!", plc_tag_decode_error(rc));
                        break;
                    }

                    /* set up the maximum request depth. */
                    (*plc)->max_requests_in_flight = max_requests_in_flight;

                    /* link up the the PLC into the global list. */
                    (*plc)->next = plcs;
                    plcs = *plc;

                    /* now the PLC can be found and the tag list is ready for use. */
                }
            } else {
                pdebug(DEBUG_WARN, "Unable to allocate Modbus PLC object!");
                rc = PLCTAG_ERR_NO_MEM;
            }
        }
    }

    /* if everything went well and it is new, set up the new PLC. */
    if(rc == PLCTAG_STATUS_OK) {
        if(is_new) {
            pdebug(DEBUG_INFO, "Creating new PLC.");

            do {
                /* we want to stay connected initially */
                (*plc)->inactivity_timeout_ms = MODBUS_INACTIVITY_TIMEOUT + time_ms();

                /* set up the PLC state */
                (*plc)->state = modbus_plc_t::PLC_CONNECT_START;

                rc = thread_create(&((*plc)->handler_thread), modbus_plc_handler, 32768, (void *)(*plc));
                if(rc != PLCTAG_STATUS_OK) {
                    pdebug(DEBUG_WARN, "Unable to create new handler thread, error %s!", plc_tag_decode_error(rc));
                    break;
                }

                pdebug(DEBUG_DETAIL, "Created thread %p.", (*plc)->handler_thread);
            } while(0);
        }
    }

    if(rc != PLCTAG_STATUS_OK && *plc) {
        pdebug(DEBUG_WARN, "PLC lookup and/or creation failed!");

        /* clean up. */
        *plc = (modbus_plc_p)rc_dec(*plc);
    }

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}


/* never enter this from within the handler thread itself! */
void modbus_plc_destructor(void *plc_arg)
{
    modbus_plc_p plc = (modbus_plc_p)plc_arg;

    pdebug(DEBUG_INFO, "Starting.");

    if(!plc) {
        pdebug(DEBUG_WARN, "Destructor called with null pointer!");
        return;
    }

    /* remove the plc from the list. */
    critical_block(mb_mutex) {
        modbus_plc_p *walker = &plcs;

        while(*walker && *walker != plc) {
            walker = &((*walker)->next);
        }

        if(*walker) {
            /* unlink the list. */
            *walker = plc->next;
            plc->next = NULL;
        } else {
            pdebug(DEBUG_WARN, "PLC not found in the list!");
        }
    }

    /* shut down the thread. */
    if(plc->handler_thread) {
        pdebug(DEBUG_DETAIL, "Terminating Modbus handler thread %p.", plc->handler_thread);

        /* set the flag to cause the thread to terminate. */
        plc->flags.terminate = 1;

        /* signal the socket to free the thread. */
        wake_plc_thread(plc);

        /* wait for the thread to terminate and destroy it. */
        thread_join(plc->handler_thread);
        thread_destroy(&plc->handler_thread);

        plc->handler_thread = NULL;
    }

    if(plc->mutex) {
        mutex_destroy(&plc->mutex);
        plc->mutex = NULL;
    }

    if(plc->sock) {
        socket_destroy(&plc->sock);
        plc->sock = NULL;
    }

    if(plc->server) {
        mem_free(plc->server);
        plc->server = NULL;
    }

    /* check to make sure we have no tags left. */
    if(plc->tag_list.head) {
        pdebug(DEBUG_WARN, "There are tags still remaining in the tag list, memory leak possible!");
    }

    pdebug(DEBUG_INFO, "Done.");
}

#define UPDATE_ERR_DELAY() \
            do { \
                err_delay = err_delay*2; \
                if(err_delay > PLC_SOCKET_ERR_MAX_DELAY) { \
                    err_delay = PLC_SOCKET_ERR_MAX_DELAY; \
                } \
                err_delay_until = (int64_t)((double)err_delay*((double)rand()/(double)(RAND_MAX))) + time_ms(); \
            } while(0)


THREAD_FUNC(modbus_plc_handler)
{
    int rc = PLCTAG_STATUS_OK;
    modbus_plc_p plc = (modbus_plc_p)arg;
    int64_t err_delay = PLC_SOCKET_ERR_START_DELAY;
    int64_t err_delay_until = 0;
    int sock_events = SOCK_EVENT_NONE;
    int waitable_events = SOCK_EVENT_NONE;

    pdebug(DEBUG_INFO, "Starting.");

    if(!plc) {
        pdebug(DEBUG_WARN, "Null PLC pointer passed!");
        THREAD_RETURN(0);
    }

    while(! plc->flags.terminate) {
        rc = tickle_all_tags(plc);
        if(rc != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Error %s tickling tags!", plc_tag_decode_error(rc));
            /* FIXME - what should we do here? */
        }

        /* if there is still a response marked ready, clean it up. */
        if(plc->flags.response_ready) {
            pdebug(DEBUG_DETAIL, "Orphan response found.");
            plc->flags.response_ready = 0;
            plc->read_data_len = 0;
        }

        switch(plc->state) {
        case modbus_plc_t::PLC_CONNECT_START:
            pdebug(DEBUG_DETAIL, "in PLC_CONNECT_START state.");

            /* connect to the PLC */
            rc = connect_plc(plc);
            if(rc == PLCTAG_STATUS_PENDING) {
                pdebug(DEBUG_DETAIL, "Socket connection process started.  Going to PLC_CONNECT_WAIT state.");
                plc->state = modbus_plc_t::PLC_CONNECT_WAIT;
            } else if(rc == PLCTAG_STATUS_OK) {
                pdebug(DEBUG_DETAIL, "Successfully connected to the PLC.  Going to PLC_READY state.");

                /* reset err_delay */
                err_delay = PLC_SOCKET_ERR_START_DELAY;

                plc->state = modbus_plc_t::PLC_READY;
            } else {
                pdebug(DEBUG_WARN, "Error %s received while starting socket connection.", plc_tag_decode_error(rc));

                socket_destroy(&(plc->sock));

                /* exponential increase with jitter. */
                UPDATE_ERR_DELAY();

                pdebug(DEBUG_WARN, "Unable to connect to the PLC, will retry later! Going to PLC_ERR_WAIT state to wait %" PRId64 "ms.", err_delay);

                plc->state = modbus_plc_t::PLC_ERR_WAIT;
            }
            break;

        case modbus_plc_t::PLC_CONNECT_WAIT:
            rc = socket_connect_tcp_check(plc->sock, SOCKET_CONNECT_TIMEOUT);
            if(rc == PLCTAG_STATUS_OK) {
                pdebug(DEBUG_DETAIL, "Socket connected, going to state PLC_READY.");

                /* we just connected, keep the connection open for a few seconds. */
                plc->inactivity_timeout_ms = MODBUS_INACTIVITY_TIMEOUT + time_ms();

                /* reset err_delay */
                err_delay = PLC_SOCKET_ERR_START_DELAY;

                plc->state = modbus_plc_t::PLC_READY;
            } else if(rc == PLCTAG_ERR_TIMEOUT) {
                pdebug(DEBUG_DETAIL, "Still waiting for socket to connect.");

                /* do not wait more.   The TCP connection check will wait in select(). */
            } else {
                pdebug(DEBUG_WARN, "Error %s received while waiting for socket connection.", plc_tag_decode_error(rc));

                socket_destroy(&(plc->sock));

                /* exponential increase with jitter. */
                UPDATE_ERR_DELAY();

                pdebug(DEBUG_WARN, "Unable to connect to the PLC, will retry later! Going to PLC_ERR_WAIT state to wait %" PRId64 "ms.", err_delay);

                plc->state = modbus_plc_t::PLC_ERR_WAIT;
            }
            break;

        case modbus_plc_t::PLC_READY:
            pdebug(DEBUG_DETAIL, "in PLC_READY state.");

            /* calculate what events we should be waiting for. */
            waitable_events = SOCK_EVENT_DEFAULT_MASK | SOCK_EVENT_CAN_READ;

            /* if there is a request queued for sending, send it. */
            if(plc->flags.request_ready) {
                waitable_events |= SOCK_EVENT_CAN_WRITE;
            }

            /* this will wait if nothing wakes it up or until it times out. */
            sock_events = socket_wait_event(plc->sock, waitable_events, MODBUS_IDLE_WAIT_TIMEOUT);

            /* check for socket errors or disconnects. */
            if((sock_events & SOCK_EVENT_ERROR) || (sock_events & SOCK_EVENT_DISCONNECT)) {
                if(sock_events & SOCK_EVENT_DISCONNECT) {
                    pdebug(DEBUG_WARN, "Unexepected socket disconnect!");
                } else {
                    pdebug(DEBUG_WARN, "Unexpected socket error!");
                }

                pdebug(DEBUG_WARN, "Going to state PLC_CONNECT_START");

                socket_destroy(&(plc->sock));

                plc->state = modbus_plc_t::PLC_CONNECT_START;
                break;
            }

            /* preference pushing requests to the PLC */
            if(sock_events & SOCK_EVENT_CAN_WRITE) {
                if(plc->flags.request_ready) {
                    pdebug(DEBUG_DETAIL, "There is a request ready to send and we can send, going to state PLC_SEND_REQUEST.");
                    plc->state = modbus_plc_t::PLC_SEND_REQUEST;
                    break;
                } else {
                    /* clear the buffer indexes just in case */
                    plc->write_data_len = 0;
                    plc->write_data_offset = 0;
                    pdebug(DEBUG_DETAIL, "Request ready state changed while we waited for the socket.");
                }
            }

            if(sock_events & SOCK_EVENT_CAN_READ) {
                pdebug(DEBUG_DETAIL, "We can receive a response going to state PLC_RECEIVE_RESPONSE.");
                plc->state = modbus_plc_t::PLC_RECEIVE_RESPONSE;
                break;
            }

            if(sock_events & SOCK_EVENT_TIMEOUT) {
                pdebug(DEBUG_DETAIL, "Timed out waiting for something to happen.");
            }

            if(sock_events & SOCK_EVENT_WAKE_UP) {
                pdebug(DEBUG_DETAIL, "Someone woke us up.");
            }

            break;

        case modbus_plc_t::PLC_SEND_REQUEST:
            debug_set_tag_id((int)plc->request_tag_id);
            pdebug(DEBUG_DETAIL, "in PLC_SEND_REQUEST state.");

            rc = send_request(plc);
            if(rc == PLCTAG_STATUS_OK) {
                pdebug(DEBUG_DETAIL, "Request sent, going to back to state PLC_READY.");

                plc->flags.request_ready = 0;
                plc->write_data_len = 0;
                plc->write_data_offset = 0;

                plc->state = modbus_plc_t::PLC_READY;
            } else if(rc == PLCTAG_STATUS_PENDING) {
                pdebug(DEBUG_DETAIL, "Not all data written, will try again.");
            } else {
                pdebug(DEBUG_WARN, "Closing socket due to write error %s.", plc_tag_decode_error(rc));

                socket_destroy(&(plc->sock));

                /* set up the state. */
                plc->flags.response_ready = 0;
                plc->flags.request_ready = 0;
                plc->read_data_len = 0;
                plc->write_data_len = 0;
                plc->write_data_offset = 0;

                /* try to reconnect immediately. */
                plc->state = modbus_plc_t::PLC_CONNECT_START;
            }

            /* if we did not send all the packet, we stay in this state and keep trying. */

            debug_set_tag_id(0);

            break;


        case modbus_plc_t::PLC_RECEIVE_RESPONSE:
            pdebug(DEBUG_DETAIL, "in PLC_RECEIVE_RESPONSE state.");

            /* get a packet */
            rc = receive_response(plc);
            if(rc == PLCTAG_STATUS_OK) {
                pdebug(DEBUG_DETAIL, "Response ready, going back to PLC_READY state.");
                plc->flags.response_ready = 1;
                plc->state = modbus_plc_t::PLC_READY;
            } else if(rc == PLCTAG_STATUS_PENDING) {
                pdebug(DEBUG_DETAIL, "Response not complete, continue reading data.");
            } else {
                pdebug(DEBUG_WARN, "Closing socket due to read error %s.", plc_tag_decode_error(rc));

                socket_destroy(&(plc->sock));

                /* set up the state. */
                plc->flags.response_ready = 0;
                plc->flags.request_ready = 0;
                plc->read_data_len = 0;
                plc->write_data_len = 0;
                plc->write_data_offset = 0;

                /* try to reconnect immediately. */
                plc->state = modbus_plc_t::PLC_CONNECT_START;
            }

            /* in all cases we want to cycle through the state machine immediately. */

            break;

        case modbus_plc_t::PLC_ERR_WAIT:
            pdebug(DEBUG_DETAIL, "in PLC_ERR_WAIT state.");

            /* clean up the socket in case we did not earlier */
            if(plc->sock) {
                socket_destroy(&(plc->sock));
            }

            /* wait until done. */
            if(err_delay_until > time_ms()) {
                pdebug(DEBUG_DETAIL, "Waiting for at least %" PRId64 "ms.", (err_delay_until - time_ms()));
                sleep_ms(PLC_SOCKET_ERR_DELAY_WAIT_INCREMENT);
            } else {
                pdebug(DEBUG_DETAIL, "Error wait is over, going to state PLC_CONNECT_START.");
                plc->state = modbus_plc_t::PLC_CONNECT_START;
            }
            break;

        default:
            pdebug(DEBUG_WARN, "Unknown state %d!", plc->state);
            plc->state = modbus_plc_t::PLC_CONNECT_START;
            break;
        }

        /* wait if needed, could be signalled already. */
        //cond_wait(plc->wait_cond, MODBUS_IDLE_WAIT_TIMEOUT);
    }

    pdebug(DEBUG_INFO, "Done.");

    THREAD_RETURN(0);
}


void wake_plc_thread(modbus_plc_p plc)
{
    pdebug(DEBUG_DETAIL, "Starting.");

    if(plc ) {
        if(plc->sock) {
            socket_wake(plc->sock);
        } else {
            pdebug(DEBUG_DETAIL, "PLC socket pointer is NULL.");
        }
    } else {
        pdebug(DEBUG_WARN, "PLC pointer is NULL!");
    }

    pdebug(DEBUG_DETAIL, "Done.");
}



int connect_plc(modbus_plc_p plc)
{
    int rc = PLCTAG_STATUS_OK;
    char **server_port = NULL;
    char *server = NULL;
    int port = MODBUS_DEFAULT_PORT;

    pdebug(DEBUG_DETAIL, "Starting.");

    server_port = str_split(plc->server, ":");
    if(!server_port) {
        pdebug(DEBUG_WARN, "Unable to split server and port string!");
        return PLCTAG_ERR_BAD_CONFIG;
    }

    if(server_port[0] == NULL) {
        pdebug(DEBUG_WARN, "Server string is malformed or empty!");
        mem_free(server_port);
        return PLCTAG_ERR_BAD_CONFIG;
    } else {
        server = server_port[0];
    }

    if(server_port[1] != NULL) {
        rc = str_to_int(server_port[1], &port);
        if(rc != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Unable to extract port number from server string \"%s\"!", plc->server);
            mem_free(server_port);
            server_port = NULL;
            return PLCTAG_ERR_BAD_CONFIG;
        }
    } else {
        port = MODBUS_DEFAULT_PORT;
    }

    pdebug(DEBUG_DETAIL, "Using server \"%s\" and port %d.", server, port);

    rc = socket_create(&(plc->sock));
    if(rc != PLCTAG_STATUS_OK) {
        /* done with the split string. */
        mem_free(server_port);
        server_port = NULL;

        pdebug(DEBUG_WARN, "Unable to create socket object, error %s!", plc_tag_decode_error(rc));
        return rc;
    }

    /* connect to the socket */
    pdebug(DEBUG_DETAIL, "Connecting to %s on port %d...", server, port);
    rc = socket_connect_tcp_start(plc->sock, server, port);
    if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
        /* done with the split string. */
        mem_free(server_port);

        pdebug(DEBUG_WARN, "Unable to connect to the server \"%s\", got error %s!", plc->server, plc_tag_decode_error(rc));
        socket_destroy(&(plc->sock));
        return rc;
    }

    /* done with the split string. */
    if(server_port) {
        mem_free(server_port);
        server_port = NULL;
    }

    /* clear the state for reading and writing. */
    plc->flags.request_ready = 0;
    plc->flags.response_ready = 0;
    plc->read_data_len = 0;
    plc->write_data_len = 0;
    plc->write_data_offset = 0;

    pdebug(DEBUG_DETAIL, "Done with status %s.", plc_tag_decode_error(rc));

    return rc;
}


int tickle_all_tags(modbus_plc_p plc)
{
    int rc = PLCTAG_STATUS_OK;
    struct modbus_tag_list_t idle_list = { NULL, NULL };
    struct modbus_tag_list_t active_list = { NULL, NULL };
    modbus_tag_p tag = NULL;

    pdebug(DEBUG_DETAIL, "Starting.");

    /*
     * The mutex prevents the list from changing, the PLC
     * from being freed, and the tags from being freed.
     */
    critical_block(plc->mutex) {
        while((tag = pop_tag(&(plc->tag_list)))) {
            int tag_pushed = 0;

            debug_set_tag_id(tag->tag_id);

            /* make sure nothing else can modify the tag while we are */
            critical_block(tag->api_mutex) {
            // if(mutex_try_lock(tag->api_mutex) == PLCTAG_STATUS_OK) {
                rc = tickle_tag(plc, tag);
                if(rc == PLCTAG_STATUS_OK) {
                    pdebug(DEBUG_SPEW, "Pushing tag onto idle list.");
                    push_tag(&idle_list, tag);
                    tag_pushed = 1;
                } else if(rc == PLCTAG_STATUS_PENDING) {
                    pdebug(DEBUG_SPEW, "Pushing tag onto active list.");
                    push_tag(&active_list, tag);
                    tag_pushed = 1;
                } else {
                    pdebug(DEBUG_WARN, "Error %s tickling tag! Pushing tag onto idle list.", plc_tag_decode_error(rc));
                    push_tag(&idle_list, tag);
                    tag_pushed = 1;
                }
            }

            if(tag_pushed) {
                /* call the callbacks outside the API mutex. */
                plc_tag_generic_handle_event_callbacks((plc_tag_p)tag);
            } else {
                pdebug(DEBUG_WARN, "Tag mutex not taken!  Doing emergency push of tag onto idle list.");
                push_tag(&idle_list, tag);
            }

            rc = PLCTAG_STATUS_OK;

            debug_set_tag_id(0);
        }

        /* merge the lists and replace the old list. */
        pdebug(DEBUG_SPEW, "Merging active and idle lists.");
        plc->tag_list = merge_lists(&active_list, &idle_list);
    }

    pdebug(DEBUG_DETAIL, "Done: %s", plc_tag_decode_error(rc));

    return rc;
}



int tickle_tag(modbus_plc_p plc, modbus_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    int op = (int)(tag->op);
    int raise_event = 0;
    int event;
    int event_status;

    pdebug(DEBUG_SPEW, "Starting.");

    switch(op) {
        case TAG_OP_IDLE:
            pdebug(DEBUG_SPEW, "Tag is idle.");
            rc = PLCTAG_STATUS_OK;
            break;

        case TAG_OP_READ_REQUEST:
            /* if the PLC is ready and there is no request queued yet, build a request. */
            if(find_request_slot(plc, tag) == PLCTAG_STATUS_OK) {
                rc = create_read_request(plc, tag);
                if(rc == PLCTAG_STATUS_OK) {
                    pdebug(DEBUG_DETAIL, "Read request created.");

                    tag->op = TAG_OP_READ_RESPONSE;
                    plc->flags.request_ready = 1;

                    rc = PLCTAG_STATUS_PENDING;
                } else {
                    pdebug(DEBUG_WARN, "Error %s creating read request!", plc_tag_decode_error(rc));

                    /* remove the tag from the request slot. */
                    clear_request_slot(plc, tag);

                    tag->op = TAG_OP_IDLE;
                    tag->read_complete = 1;
                    tag->read_in_flight = 0;
                    tag->status = (int8_t)rc;

                    raise_event = 1;
                    event = PLCTAG_EVENT_READ_COMPLETED;
                    event_status = rc;

                    //plc_tag_tickler_wake();
                    plc_tag_generic_wake_tag((plc_tag_p)tag);
                    rc = PLCTAG_STATUS_OK;
                }
            } else {
                pdebug(DEBUG_SPEW, "Request already in flight or PLC not ready, waiting for next chance.");
                rc = PLCTAG_STATUS_PENDING;
            }
            break;

        case TAG_OP_READ_RESPONSE:
            /* cross check the state. */
            if(plc->state == modbus_plc_t::PLC_CONNECT_START
            || plc->state == modbus_plc_t::PLC_CONNECT_WAIT
            || plc->state == modbus_plc_t::PLC_ERR_WAIT) {
                pdebug(DEBUG_WARN, "PLC changed state, restarting request.");
                tag->op = TAG_OP_READ_REQUEST;
                break;
            }

            if(plc->flags.response_ready) {
                rc = check_read_response(plc, tag);
                switch(rc) {
                    case PLCTAG_ERR_PARTIAL:
                        /* partial response, keep going */
                        pdebug(DEBUG_DETAIL, "Found our response, but we are not done.");

                        /* remove the tag from the request slot. */
                        clear_request_slot(plc, tag);

                        plc->flags.response_ready = 0;
                        tag->op = TAG_OP_READ_REQUEST;

                        rc = PLCTAG_STATUS_OK;
                        break;

                    case PLCTAG_ERR_NO_MATCH:
                        pdebug(DEBUG_SPEW, "Not our response.");
                        rc = PLCTAG_STATUS_PENDING;
                        break;

                    case PLCTAG_STATUS_OK:
                        /* fall through */
                    default:
                        /* set the status before we might change it. */
                        tag->status = (int8_t)rc;

                        if(rc == PLCTAG_STATUS_OK) {
                            pdebug(DEBUG_DETAIL, "Found our response.");
                            plc->flags.response_ready = 0;
                        } else {
                            pdebug(DEBUG_WARN, "Error %s checking read response!", plc_tag_decode_error(rc));
                            rc = PLCTAG_STATUS_OK;
                        }

                        /* remove the tag from the request slot. */
                        clear_request_slot(plc, tag);

                        plc->flags.response_ready = 0;
                        tag->op = TAG_OP_IDLE;
                        tag->read_in_flight = 0;
                        tag->read_complete = 1;
                        tag->status = (int8_t)rc;

                        raise_event = 1;
                        event = PLCTAG_EVENT_READ_COMPLETED;
                        event_status = rc;

                        /* tell the world we are done. */
                        //plc_tag_tickler_wake();
                        plc_tag_generic_wake_tag((plc_tag_p)tag);

                        break;
                }
            } else {
                pdebug(DEBUG_SPEW, "No response yet, Continue waiting.");
                rc = PLCTAG_STATUS_PENDING;
            }
            break;

        case TAG_OP_WRITE_REQUEST:
            /* if the PLC is ready and there is no request queued yet, build a request. */
            if(find_request_slot(plc, tag) == PLCTAG_STATUS_OK) {
                rc = create_write_request(plc, tag);
                if(rc == PLCTAG_STATUS_OK) {
                    pdebug(DEBUG_DETAIL, "Write request created.");

                    tag->op = TAG_OP_WRITE_RESPONSE;
                    plc->flags.request_ready = 1;

                    rc = PLCTAG_STATUS_PENDING;
                } else {
                    pdebug(DEBUG_WARN, "Error %s creating write request!", plc_tag_decode_error(rc));

                    /* remove the tag from the request slot. */
                    clear_request_slot(plc, tag);

                    tag->op = TAG_OP_IDLE;
                    tag->write_complete = 1;
                    tag->write_in_flight = 0;
                    tag->status = (int8_t)rc;

                    raise_event = 1;
                    event = PLCTAG_EVENT_WRITE_COMPLETED;
                    event_status = rc;

                    //plc_tag_tickler_wake();
                    plc_tag_generic_wake_tag((plc_tag_p)tag);

                    rc = PLCTAG_STATUS_OK;
                }
            } else {
                pdebug(DEBUG_SPEW, "Request already in flight or PLC not ready, waiting for next chance.");
                rc = PLCTAG_STATUS_PENDING;
            }
            break;

        case TAG_OP_WRITE_RESPONSE:
            /* cross check the state. */
            if(plc->state == modbus_plc_t::PLC_CONNECT_START
            || plc->state == modbus_plc_t::PLC_CONNECT_WAIT
            || plc->state == modbus_plc_t::PLC_ERR_WAIT) {
                pdebug(DEBUG_WARN, "PLC changed state, restarting request.");
                tag->op = TAG_OP_WRITE_REQUEST;
                break;
            }

            if(plc->flags.response_ready) {
                rc = check_write_response(plc, tag);
                if(rc == PLCTAG_STATUS_OK) {
                    pdebug(DEBUG_DETAIL, "Found our response.");

                    /* remove the tag from the request slot. */
                    clear_request_slot(plc, tag);

                    plc->flags.response_ready = 0;
                    tag->op = TAG_OP_IDLE;
                    tag->write_complete = 1;
                    tag->write_in_flight = 0;
                    tag->status = (int8_t)rc;

                    raise_event = 1;
                    event = PLCTAG_EVENT_WRITE_COMPLETED;
                    event_status = rc;

                    /* tell the world we are done. */
                    //plc_tag_tickler_wake();
                    plc_tag_generic_wake_tag((plc_tag_p)tag);

                    rc = PLCTAG_STATUS_OK;
                } else if(rc == PLCTAG_ERR_PARTIAL) {
                    pdebug(DEBUG_DETAIL, "Found our response, but we are not done.");

                    plc->flags.response_ready = 0;
                    tag->op = TAG_OP_WRITE_REQUEST;

                    rc = PLCTAG_STATUS_OK;
                } else if(rc == PLCTAG_ERR_NO_MATCH) {
                    pdebug(DEBUG_SPEW, "Not our response.");
                    rc = PLCTAG_STATUS_PENDING;
                } else {
                    pdebug(DEBUG_WARN, "Error %s checking write response!", plc_tag_decode_error(rc));

                    /* remove the tag from the request slot. */
                    clear_request_slot(plc, tag);

                    plc->flags.response_ready = 0;
                    tag->op = TAG_OP_IDLE;
                    tag->write_complete = 1;
                    tag->write_in_flight = 0;
                    tag->status = (int8_t)rc;

                    raise_event = 1;
                    event = PLCTAG_EVENT_WRITE_COMPLETED;
                    event_status = rc;

                    /* tell the world we are done. */
                    //plc_tag_tickler_wake();
                    plc_tag_generic_wake_tag((plc_tag_p)tag);

                    rc = PLCTAG_STATUS_OK;
                }
            } else {
                pdebug(DEBUG_SPEW, "No response yet, Continue waiting.");
                rc = PLCTAG_STATUS_PENDING;
            }
            break;

        default:
            pdebug(DEBUG_WARN, "Unknown tag operation %d!", op);

            tag->op = TAG_OP_IDLE;
            tag->status = (int8_t)PLCTAG_ERR_NOT_IMPLEMENTED;

            /* tell the world we are done. */
            //plc_tag_tickler_wake();
            plc_tag_generic_wake_tag((plc_tag_p)tag);

            rc = PLCTAG_STATUS_OK;
            break;
    }

    if(raise_event) {
        tag_raise_event((plc_tag_p)tag, event, (int8_t)event_status);
        plc_tag_generic_handle_event_callbacks((plc_tag_p)tag);
    }
    
    /*
     * Call the generic tag tickler function to handle auto read/write and set
     * up events.
     */
    plc_tag_generic_tickler((plc_tag_p)tag);

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}




int find_request_slot(modbus_plc_p plc, modbus_tag_p tag)
{
    pdebug(DEBUG_SPEW, "Starting.");

    if(plc->flags.request_ready) {
        pdebug(DEBUG_DETAIL, "There is a request already queued for sending.");
        return PLCTAG_ERR_BUSY;
    }

    if(plc->state != modbus_plc_t::PLC_READY) {
        pdebug(DEBUG_DETAIL, "PLC not ready.");
        return PLCTAG_ERR_BUSY;
    }

    if(tag->tag_id == 0) {
        pdebug(DEBUG_DETAIL, "Tag not ready.");
        return PLCTAG_ERR_BUSY;
    }

    /* search for a slot. */
    for(int slot=0; slot < plc->max_requests_in_flight; slot++) {
        if(plc->tags_with_requests[slot] == 0) {
            pdebug(DEBUG_DETAIL, "Found request slot %d for tag %" PRId32 ".", slot, tag->tag_id);
            plc->tags_with_requests[slot] = tag->tag_id;
            tag->request_slot = slot;
            return PLCTAG_STATUS_OK;
        }
    }

    pdebug(DEBUG_SPEW, "Done.");

    return PLCTAG_ERR_NO_RESOURCES;
}


void clear_request_slot(modbus_plc_p plc, modbus_tag_p tag)
{
    pdebug(DEBUG_DETAIL, "Starting for tag %" PRId32 ".", tag->tag_id);

    /* find the tag in the slots. */
    for(int slot=0; slot < plc->max_requests_in_flight; slot++) {
        if(plc->tags_with_requests[slot] == tag->tag_id) {
            pdebug(DEBUG_DETAIL, "Found tag %" PRId32 " in slot %d.", tag->tag_id, slot);

            if(slot != tag->request_slot) {
                pdebug(DEBUG_DETAIL, "Tag was not in expected slot %d!", tag->request_slot);
            }

            plc->tags_with_requests[slot] = 0;
            tag->request_slot = -1;
        }
    }

    pdebug(DEBUG_DETAIL, "Done for tag %" PRId32 ".", tag->tag_id);
}



int receive_response(modbus_plc_p plc)
{
    int rc = 0;
    int data_needed = 0;

    pdebug(DEBUG_DETAIL, "Starting.");

    /* socket could be closed due to inactivity. */
    if(!plc->sock) {
        pdebug(DEBUG_SPEW, "Socket is closed or missing.");
        return PLCTAG_STATUS_OK;
    }

    do {
        /* how much data do we need? */
        if(plc->read_data_len >= MODBUS_MBAP_SIZE) {
            int packet_size = plc->read_data[5] + (plc->read_data[4] << 8);
            data_needed = (MODBUS_MBAP_SIZE + packet_size) - plc->read_data_len;

            pdebug(DEBUG_DETAIL, "Packet header read, data_needed=%d, packet_size=%d, read_data_len=%d", data_needed, packet_size, plc->read_data_len);

            if(data_needed > PLC_READ_DATA_LEN) {
                pdebug(DEBUG_WARN, "Error, packet size, %d, greater than buffer size, %d!", data_needed, PLC_READ_DATA_LEN);
                return PLCTAG_ERR_TOO_LARGE;
            } else if(data_needed < 0) {
                pdebug(DEBUG_WARN, "Read more than a packet!  Expected %d bytes, but got %d bytes!", (MODBUS_MBAP_SIZE + packet_size), plc->read_data_len);
                return PLCTAG_ERR_TOO_LARGE;
            }
        } else {
            data_needed = MODBUS_MBAP_SIZE - plc->read_data_len;
            pdebug(DEBUG_DETAIL, "Still reading packet header, data_needed=%d, read_data_len=%d", data_needed, plc->read_data_len);
        }

        if(data_needed == 0) {
            pdebug(DEBUG_DETAIL, "Got all data needed.");
            break;
        }

        /* read the socket. */
        rc = socket_read(plc->sock, plc->read_data + plc->read_data_len, data_needed, SOCKET_READ_TIMEOUT);
        if(rc >= 0) {
            /* got data! Or got nothing, but no error. */
            plc->read_data_len += rc;

            pdebug_dump_bytes(DEBUG_SPEW, plc->read_data, plc->read_data_len);
        } else if(rc == PLCTAG_ERR_TIMEOUT) {
            pdebug(DEBUG_DETAIL, "Done. Socket read timed out.");
            return PLCTAG_STATUS_PENDING;
        } else {
            pdebug(DEBUG_WARN, "Error, %s, reading socket!", plc_tag_decode_error(rc));
            return rc;
        }

        pdebug(DEBUG_DETAIL, "After reading the socket, total read=%d and data needed=%d.", plc->read_data_len, data_needed);
    } while(rc > 0);


    if(data_needed == 0) {
        /* we got our packet. */
        pdebug(DEBUG_DETAIL, "Received full packet.");
        pdebug_dump_bytes(DEBUG_DETAIL, plc->read_data, plc->read_data_len);
        plc->flags.response_ready = 1;

        rc = PLCTAG_STATUS_OK;
    } else {
        /* data_needed is greater than zero. */
        pdebug(DEBUG_DETAIL, "Received partial packet of %d bytes of %d.", plc->read_data_len, (data_needed + plc->read_data_len));
        rc = PLCTAG_STATUS_PENDING;
    }

    /* if we have some data in the buffer, keep the connection open. */
    if(plc->read_data_len > 0) {
        plc->inactivity_timeout_ms = MODBUS_INACTIVITY_TIMEOUT + time_ms();
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}



int send_request(modbus_plc_p plc)
{
    int rc = 1;
    int data_left = plc->write_data_len - plc->write_data_offset;

    pdebug(DEBUG_DETAIL, "Starting.");

    /* check socket, could be closed due to inactivity. */
    if(!plc->sock) {
        pdebug(DEBUG_DETAIL, "No socket or socket is closed.");
        return PLCTAG_ERR_BAD_CONNECTION;
    }

    /* if we have some data in the buffer, keep the connection open. */
    if(plc->write_data_len > 0) {
        plc->inactivity_timeout_ms = MODBUS_INACTIVITY_TIMEOUT + time_ms();
    }

    /* is there anything to do? */
    if(! plc->flags.request_ready) {
        pdebug(DEBUG_WARN, "No packet to send!");
        return PLCTAG_ERR_NO_DATA;
    }

    /* try to send some data. */
    rc = socket_write(plc->sock, plc->write_data + plc->write_data_offset, data_left, SOCKET_WRITE_TIMEOUT);
    if(rc >= 0) {
        plc->write_data_offset += rc;
        data_left = plc->write_data_len - plc->write_data_offset;
    } else if(rc == PLCTAG_ERR_TIMEOUT) {
        pdebug(DEBUG_DETAIL, "Done.  Timeout writing to socket.");
        rc = PLCTAG_STATUS_OK;
    } else {
        pdebug(DEBUG_WARN, "Error, %s, writing to socket!", plc_tag_decode_error(rc));
        return rc;
    }

    /* clean up if full write was done. */
    if(data_left == 0) {
        pdebug(DEBUG_DETAIL, "Full packet written.");
        pdebug_dump_bytes(DEBUG_DETAIL, plc->write_data, plc->write_data_len);

        // plc->flags.request_ready = 0;
        plc->write_data_len = 0;
        plc->write_data_offset = 0;
        plc->response_tag_id = plc->request_tag_id;
        plc->request_tag_id = 0;

        rc = PLCTAG_STATUS_OK;
    } else {
        pdebug(DEBUG_DETAIL, "Partial packet written.");
        rc = PLCTAG_STATUS_PENDING;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}




int create_read_request(modbus_plc_p plc, modbus_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    uint16_t seq_id = (++(plc->seq_id) ? plc->seq_id : ++(plc->seq_id)); // disallow zero
    int registers_per_request = (MAX_MODBUS_RESPONSE_PAYLOAD * 8) / tag->elem_size;
    int base_register = tag->reg_base + (tag->request_num * registers_per_request);
    int register_count = tag->elem_count - (tag->request_num * registers_per_request);

    pdebug(DEBUG_DETAIL, "Starting.");

    pdebug(DEBUG_DETAIL, "seq_id=%d", seq_id);
    pdebug(DEBUG_DETAIL, "registers_per_request = %d", registers_per_request);
    pdebug(DEBUG_DETAIL, "base_register = %d", base_register);
    pdebug(DEBUG_DETAIL, "register_count = %d", register_count);

    /* clamp the number of registers we ask for to what will fit. */
    if(register_count > registers_per_request) {
        register_count = registers_per_request;
    }

    pdebug(DEBUG_INFO, "preparing read request for %d registers (of %d total) from base register %d.", register_count, tag->elem_count, base_register);

    /* build the read request.
     *    Byte  Meaning
     *      0    High byte of request sequence ID.
     *      1    Low byte of request sequence ID.
     *      2    High byte of the protocol version identifier (zero).
     *      3    Low byte of the protocol version identifier (zero).
     *      4    High byte of the message length.
     *      5    Low byte of the message length.
     *      6    Device address.
     *      7    Function code.
     *      8    High byte of first register address.
     *      9    Low byte of the first register address.
     *     10    High byte of the register count.
     *     11    Low byte of the register count.
     */

    plc->write_data_len = 0;

    /* build the request sequence ID */
    plc->write_data[plc->write_data_len] = (uint8_t)((seq_id >> 8) & 0xFF); plc->write_data_len++;
    plc->write_data[plc->write_data_len] = (uint8_t)((seq_id >> 0) & 0xFF); plc->write_data_len++;

    /* protocol version is always zero */
    plc->write_data[plc->write_data_len] = 0; plc->write_data_len++;
    plc->write_data[plc->write_data_len] = 0; plc->write_data_len++;

    /* request packet length */
    plc->write_data[plc->write_data_len] = 0; plc->write_data_len++;
    plc->write_data[plc->write_data_len] = 6; plc->write_data_len++;

    /* device address */
    plc->write_data[plc->write_data_len] = plc->server_id; plc->write_data_len++;

    /* function code depends on the register type. */
    switch(tag->reg_type) {
        case MB_REG_COIL:
            plc->write_data[7] = MB_CMD_READ_COIL_MULTI; plc->write_data_len++;
            break;

        case MB_REG_DISCRETE_INPUT:
            plc->write_data[7] = MB_CMD_READ_DISCRETE_INPUT_MULTI; plc->write_data_len++;
            break;

        case MB_REG_HOLDING_REGISTER:
            plc->write_data[7] = MB_CMD_READ_HOLDING_REGISTER_MULTI; plc->write_data_len++;
            break;

        case MB_REG_INPUT_REGISTER:
            plc->write_data[7] = MB_CMD_READ_INPUT_REGISTER_MULTI; plc->write_data_len++;
            break;

        default:
            pdebug(DEBUG_WARN, "Unsupported register type %d!", tag->reg_type);
            return PLCTAG_ERR_UNSUPPORTED;
            break;
    }

    /* register base. */
    plc->write_data[plc->write_data_len] = (uint8_t)((base_register >> 8) & 0xFF); plc->write_data_len++;
    plc->write_data[plc->write_data_len] = (uint8_t)((base_register >> 0) & 0xFF); plc->write_data_len++;

    /* number of elements to read. */
    plc->write_data[plc->write_data_len] = (uint8_t)((register_count >> 8) & 0xFF); plc->write_data_len++;
    plc->write_data[plc->write_data_len] = (uint8_t)((register_count >> 0) & 0xFF); plc->write_data_len++;

    tag->seq_id = seq_id;
    plc->flags.request_ready = 1;
    plc->request_tag_id = tag->tag_id;

    pdebug(DEBUG_DETAIL, "Created read request:");
    pdebug_dump_bytes(DEBUG_DETAIL, plc->write_data, plc->write_data_len);

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}




/* Read response.
 *    Byte  Meaning
 *      0    High byte of request sequence ID.
 *      1    Low byte of request sequence ID.
 *      2    High byte of the protocol version identifier (zero).
 *      3    Low byte of the protocol version identifier (zero).
 *      4    High byte of the message length.
 *      5    Low byte of the message length.
 *      6    Device address.
 *      7    Function code.
 *      8    First byte of the result.
 *      ...  up to 253 bytes of payload.
 */


int check_read_response(modbus_plc_p plc, modbus_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    uint16_t seq_id = (uint16_t)((uint16_t)plc->read_data[1] +(uint16_t)(plc->read_data[0] << 8));
    int partial_read = 0;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(seq_id == tag->seq_id) {
        uint8_t has_error = plc->read_data[7] & (uint8_t)0x80;

        /* the operation is complete regardless of the outcome. */
        //tag->flags.operation_complete = 1;

        if(has_error) {
            rc = translate_modbus_error(plc->read_data[8]);

            pdebug(DEBUG_WARN, "Got read response %ud, with error %s, of length %d.", (int)(unsigned int)seq_id, plc_tag_decode_error(rc), plc->read_data_len);
        } else {
            int registers_per_request = (MAX_MODBUS_RESPONSE_PAYLOAD * 8) / tag->elem_size;
            int register_offset = (tag->request_num * registers_per_request);
            int byte_offset = (register_offset * tag->elem_size) / 8;
            uint8_t payload_size = plc->read_data[8];
            int copy_size = ((tag->size - byte_offset) < payload_size ? (tag->size - byte_offset) : payload_size);

            /* no error. So copy the data. */
            pdebug(DEBUG_DETAIL, "Got read response %u of length %d with payload of size %d.", (int)(unsigned int)seq_id, plc->read_data_len, payload_size);
            pdebug(DEBUG_DETAIL, "registers_per_request = %d", registers_per_request);
            pdebug(DEBUG_DETAIL, "register_offset = %d", register_offset);
            pdebug(DEBUG_DETAIL, "byte_offset = %d", byte_offset);
            pdebug(DEBUG_DETAIL, "copy_size = %d", copy_size);

            mem_copy(tag->data + byte_offset, &plc->read_data[9], copy_size);

            /* are we done? */
            if(tag->size > (byte_offset + copy_size)) {
                /* Not yet. */
                pdebug(DEBUG_DETAIL, "Not done reading entire tag.");
                partial_read = 1;
            } else {
                /* read is done. */
                pdebug(DEBUG_DETAIL, "Read is complete.");
                partial_read = 0;
            }

            rc = PLCTAG_STATUS_OK;
        }

        /* either way, clean up the PLC buffer. */
        plc->read_data_len = 0;
        plc->flags.response_ready = 0;

        /* clean up tag*/
        if(!partial_read) {
            pdebug(DEBUG_DETAIL, "Read is complete.  Cleaning up tag state.");
            tag->seq_id = 0;
            tag->read_complete = 1;
            tag->read_in_flight = 0;
            tag->status = (int8_t)rc;
            tag->request_num = 0;
        } else {
            pdebug(DEBUG_DETAIL, "Read is partially complete.  We need to do at least one more request.");
            rc = PLCTAG_ERR_PARTIAL;
            tag->request_num++;
            tag->status = (int8_t)PLCTAG_STATUS_PENDING;
        }
    } else {
        pdebug(DEBUG_DETAIL, "Not our response.");

        rc = PLCTAG_ERR_NO_MATCH;
    }

    pdebug(DEBUG_DETAIL, "Done: %s", plc_tag_decode_error(rc));

    return rc;
}



/* build the write request.
 *    Byte  Meaning
 *      0    High byte of request sequence ID.
 *      1    Low byte of request sequence ID.
 *      2    High byte of the protocol version identifier (zero).
 *      3    Low byte of the protocol version identifier (zero).
 *      4    High byte of the message length.
 *      5    Low byte of the message length.
 *      6    Device address.
 *      7    Function code.
 *      8    High byte of first register address.
 *      9    Low byte of the first register address.
 *     10    High byte of the register count.
 *     11    Low byte of the register count.
 *     12    Number of bytes of data to write.
 *     13... Data bytes.
 */

int create_write_request(modbus_plc_p plc, modbus_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    uint16_t seq_id = (++(plc->seq_id) ? plc->seq_id : ++(plc->seq_id)); // disallow zero
    int registers_per_request = (MAX_MODBUS_REQUEST_PAYLOAD * 8) / tag->elem_size;
    int base_register = tag->reg_base + (tag->request_num * registers_per_request);
    int register_count = tag->elem_count - (tag->request_num * registers_per_request);
    int register_offset = (tag->request_num * registers_per_request);
    int byte_offset = (register_offset * tag->elem_size) / 8;
    int request_payload_size = 0;

    pdebug(DEBUG_DETAIL, "Starting.");

    pdebug(DEBUG_SPEW, "seq_id=%d", seq_id);
    pdebug(DEBUG_SPEW, "registers_per_request = %d", registers_per_request);
    pdebug(DEBUG_SPEW, "base_register = %d", base_register);
    pdebug(DEBUG_SPEW, "register_count = %d", register_count);
    pdebug(DEBUG_SPEW, "register_offset = %d", register_offset);
    pdebug(DEBUG_SPEW, "byte_offset = %d", byte_offset);

    /* clamp the number of registers we ask for to what will fit. */
    if(register_count > registers_per_request) {
        register_count = registers_per_request;
    }

    /* how many bytes, rounded up to the nearest byte. */
    request_payload_size = ((register_count * tag->elem_size) + 7) / 8;

    pdebug(DEBUG_DETAIL, "preparing write request for %d registers (of %d total) from base register %d of payload size %d in bytes.", register_count, tag->elem_count, base_register, request_payload_size);

    /* FIXME - remove this when we figure out how to push multiple requests. */
    plc->write_data_len = 0;

    /* build the request sequence ID */
    plc->write_data[plc->write_data_len] = (uint8_t)((seq_id >> 8) & 0xFF); plc->write_data_len++;
    plc->write_data[plc->write_data_len] = (uint8_t)((seq_id >> 0) & 0xFF); plc->write_data_len++;

    /* protocol version is always zero */
    plc->write_data[plc->write_data_len] = 0; plc->write_data_len++;
    plc->write_data[plc->write_data_len] = 0; plc->write_data_len++;

    /* request packet length */
    plc->write_data[plc->write_data_len] = (uint8_t)(((request_payload_size + 7) >> 8) & 0xFF); plc->write_data_len++;
    plc->write_data[plc->write_data_len] = (uint8_t)(((request_payload_size + 7) >> 0) & 0xFF); plc->write_data_len++;

    /* device address */
    plc->write_data[plc->write_data_len] = plc->server_id; plc->write_data_len++;

    /* function code depends on the register type. */
    switch(tag->reg_type) {
        case MB_REG_COIL:
            plc->write_data[7] = MB_CMD_WRITE_COIL_MULTI; plc->write_data_len++;
            break;

        case MB_REG_DISCRETE_INPUT:
            pdebug(DEBUG_WARN, "Done. You cannot write a discrete input!");
            return PLCTAG_ERR_UNSUPPORTED;
            break;

        case MB_REG_HOLDING_REGISTER:
            plc->write_data[7] = MB_CMD_WRITE_HOLDING_REGISTER_MULTI; plc->write_data_len++;
            break;

        case MB_REG_INPUT_REGISTER:
            pdebug(DEBUG_WARN, "Done. You cannot write an analog input!");
            return PLCTAG_ERR_UNSUPPORTED;
            break;

        default:
            pdebug(DEBUG_WARN, "Done. Unsupported register type %d!", tag->reg_type);
            return PLCTAG_ERR_UNSUPPORTED;
            break;
    }

    /* register base. */
    plc->write_data[plc->write_data_len] = (uint8_t)((base_register >> 8) & 0xFF); plc->write_data_len++;
    plc->write_data[plc->write_data_len] = (uint8_t)((base_register >> 0) & 0xFF); plc->write_data_len++;

    /* number of elements to read. */
    plc->write_data[plc->write_data_len] = (uint8_t)((register_count >> 8) & 0xFF); plc->write_data_len++;
    plc->write_data[plc->write_data_len] = (uint8_t)((register_count >> 0) & 0xFF); plc->write_data_len++;

    /* number of bytes of data to write. */
    plc->write_data[plc->write_data_len] = (uint8_t)(unsigned int)(request_payload_size); plc->write_data_len++;

    /* copy the tag data. */
    mem_copy(&plc->write_data[plc->write_data_len], &tag->data[byte_offset], request_payload_size);
    plc->write_data_len += request_payload_size;

    tag->seq_id = (uint16_t)(unsigned int)seq_id;
    plc->flags.request_ready = 1;
    plc->request_tag_id = tag->tag_id;

    pdebug(DEBUG_DETAIL, "Created write request:");
    pdebug_dump_bytes(DEBUG_DETAIL, plc->write_data, plc->write_data_len);

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}





/* Write response.
 *    Byte  Meaning
 *      0    High byte of request sequence ID.
 *      1    Low byte of request sequence ID.
 *      2    High byte of the protocol version identifier (zero).
 *      3    Low byte of the protocol version identifier (zero).
 *      4    High byte of the message length.
 *      5    Low byte of the message length.
 *      6    Device address.
 *      7    Function code.
 *      8    High byte of first register address/Error code.
 *      9    Low byte of the first register address.
 *     10    High byte of the register count.
 *     11    Low byte of the register count.
 */

int check_write_response(modbus_plc_p plc, modbus_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    uint16_t seq_id = (uint16_t)((uint16_t)plc->read_data[1] + (uint16_t)(plc->read_data[0] << 8));
    int partial_write = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    if(seq_id == tag->seq_id) {
        uint8_t has_error = plc->read_data[7] & (uint8_t)0x80;

        /* this is our response, so the operation is complete regardless of the status. */
        //tag->flags.operation_complete = 1;

        if(has_error) {
            rc = translate_modbus_error(plc->read_data[8]);

            pdebug(DEBUG_WARN, "Got write response %ud, with error %s, of length %d.", (int)(unsigned int)seq_id, plc_tag_decode_error(rc), plc->read_data_len);
        } else {
            int registers_per_request = (MAX_MODBUS_RESPONSE_PAYLOAD * 8) / tag->elem_size;
            int next_register_offset = ((tag->request_num+1) * registers_per_request);
            int next_byte_offset = (next_register_offset * tag->elem_size) / 8;

            /* no error. So copy the data. */
            pdebug(DEBUG_DETAIL, "registers_per_request = %d", registers_per_request);
            pdebug(DEBUG_DETAIL, "next_register_offset = %d", next_register_offset);
            pdebug(DEBUG_DETAIL, "next_byte_offset = %d", next_byte_offset);

            /* are we done? */
            if(tag->size > next_byte_offset) {
                /* Not yet. */
                pdebug(DEBUG_SPEW, "Not done writing entire tag.");
                partial_write = 1;
            } else {
                /* read is done. */
                pdebug(DEBUG_DETAIL, "Write is complete.");
                partial_write = 0;
            }

            rc = PLCTAG_STATUS_OK;
        }

        /* either way, clean up the PLC buffer. */
        plc->read_data_len = 0;
        plc->flags.response_ready = 0;

        /* clean up tag*/
        if(!partial_write) {
            pdebug(DEBUG_DETAIL, "Write complete. Cleaning up tag state.");
            tag->seq_id = 0;
            tag->request_num = 0;
            tag->write_complete = 1;
            tag->write_in_flight = 0;
            tag->status = (int8_t)rc;
        } else {
            pdebug(DEBUG_DETAIL, "Write partially complete.  We need to do at least one more write request.");
            rc = PLCTAG_ERR_PARTIAL;
            tag->request_num++;
            tag->status = (int8_t)PLCTAG_STATUS_PENDING;
        }
    } else {
        pdebug(DEBUG_SPEW, "Not our response.");

        rc = PLCTAG_STATUS_PENDING;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}



int translate_modbus_error(uint8_t err_code)
{
    int rc = PLCTAG_STATUS_OK;

    switch(err_code) {
        case 0x01:
            pdebug(DEBUG_WARN, "The received function code can not be processed!");
            rc = PLCTAG_ERR_UNSUPPORTED;
            break;

        case 0x02:
            pdebug(DEBUG_WARN, "The data address specified in the request is not available!");
            rc = PLCTAG_ERR_NOT_FOUND;
            break;

        case 0x03:
            pdebug(DEBUG_WARN, "The value contained in the query data field is an invalid value!");
            rc = PLCTAG_ERR_BAD_PARAM;
            break;

        case 0x04:
            pdebug(DEBUG_WARN, "An unrecoverable error occurred while the server attempted to perform the requested action!");
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;

        case 0x05:
            pdebug(DEBUG_WARN, "The server will take a long time processing this request!");
            rc = PLCTAG_ERR_PARTIAL;
            break;

        case 0x06:
            pdebug(DEBUG_WARN, "The server is busy!");
            rc = PLCTAG_ERR_BUSY;
            break;

        case 0x07:
            pdebug(DEBUG_WARN, "The server can not execute the program function specified in the request!");
            rc = PLCTAG_ERR_UNSUPPORTED;
            break;

        case 0x08:
            pdebug(DEBUG_WARN, "The slave detected a parity error when reading the extended memory!");
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;

        default:
            pdebug(DEBUG_WARN, "Unknown error response %u received!", (int)(unsigned int)(err_code));
            rc = PLCTAG_ERR_UNSUPPORTED;
            break;
    }

    return rc;
}




int parse_register_name(attr attribs, modbus_reg_type_t *reg_type, int *reg_base)
{
    int rc = PLCTAG_STATUS_OK;
    const char *reg_name = attr_get_str(attribs, "name", NULL);

    pdebug(DEBUG_INFO, "Starting.");

    if(!reg_name || str_length(reg_name)<3) {
        pdebug(DEBUG_WARN, "Incorrect or unsupported register name!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* see if we can parse the register number. */
    rc = str_to_int(&reg_name[2], reg_base);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to parse register number!");
        *reg_base = 0;
        *reg_type = MB_REG_UNKNOWN;
        return rc;
    }

    /* get the register type. */
    if(str_cmp_i_n(reg_name, "co", 2) == 0) {
        pdebug(DEBUG_DETAIL, "Found coil type.");
        *reg_type = MB_REG_COIL;
    } else if(str_cmp_i_n(reg_name, "di", 2) == 0) {
        pdebug(DEBUG_DETAIL, "Found discrete input type.");
        *reg_type = MB_REG_DISCRETE_INPUT;
    } else if(str_cmp_i_n(reg_name, "hr", 2) == 0) {
        pdebug(DEBUG_DETAIL, "Found holding register type.");
        *reg_type = MB_REG_HOLDING_REGISTER;
    } else if(str_cmp_i_n(reg_name, "ir", 2) == 0) {
        pdebug(DEBUG_DETAIL, "Found input register type.");
        *reg_type = MB_REG_INPUT_REGISTER;
    } else {
        pdebug(DEBUG_WARN, "Unknown register type, %s!", reg_name);
        *reg_base = 0;
        *reg_type = MB_REG_UNKNOWN;
        return PLCTAG_ERR_BAD_PARAM;
    }

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_OK;
}



modbus_tag_p pop_tag(modbus_tag_list_p list)
{
    modbus_tag_p tmp = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    tmp = list->head;

    /* unlink */
    if(tmp) {
        list->head = tmp->next;
        tmp->next = NULL;
    }

    /* if we removed the last element, then set the tail to NULL. */
    if(!list->head) {
        list->tail = NULL;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return tmp;
}



void push_tag(modbus_tag_list_p list, modbus_tag_p tag)
{
    pdebug(DEBUG_SPEW, "Starting.");

    tag->next = NULL;

    if(list->tail) {
        list->tail->next = tag;
        list->tail = tag;
    } else {
        /* nothing in the list. */
        list->head = tag;
        list->tail = tag;
    }

    pdebug(DEBUG_SPEW, "Done.");
}



struct modbus_tag_list_t merge_lists(modbus_tag_list_p first, modbus_tag_list_p last)
{
    struct modbus_tag_list_t result;

    pdebug(DEBUG_SPEW, "Starting.");

    if(first->head) {
        modbus_tag_p tmp = first->head;

        pdebug(DEBUG_SPEW, "First list:");
        while(tmp) {
            pdebug(DEBUG_SPEW, "  tag %d", tmp->tag_id);
            tmp = tmp->next;
        }
    } else {
        pdebug(DEBUG_SPEW, "First list: empty.");
    }

    if(last->head) {
        modbus_tag_p tmp = last->head;

        pdebug(DEBUG_SPEW, "Second list:");
        while(tmp) {
            pdebug(DEBUG_SPEW, "  tag %d", tmp->tag_id);
            tmp = tmp->next;
        }
    } else {
        pdebug(DEBUG_SPEW, "Second list: empty.");
    }

    /* set up the head of the new list. */
    result.head = (first->head ? first->head : last->head);

    /* stitch up the tail of the first list. */
    if(first->tail) {
        first->tail->next = last->head;
    }

    /* set up the tail of the new list */
    result.tail = (last->tail ? last->tail : first->tail);

    /* make sure the old lists do not reference the tags. */
    first->head = NULL;
    first->tail = NULL;
    last->head = NULL;
    last->tail = NULL;

    if(result.head) {
        modbus_tag_p tmp = result.head;

        pdebug(DEBUG_SPEW, "Result list:");
        while(tmp) {
            pdebug(DEBUG_SPEW, "  tag %d", tmp->tag_id);
            tmp = tmp->next;
        }
    } else {
        pdebug(DEBUG_SPEW, "Result list: empty.");
    }

    pdebug(DEBUG_SPEW, "Done.");

    return result;
}



int remove_tag(modbus_tag_list_p list, modbus_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    modbus_tag_p cur = list->head;
    modbus_tag_p prev = NULL;

    pdebug(DEBUG_DETAIL, "Starting.");

    while(cur && cur != tag) {
        prev = cur;
        cur = cur->next;
    }

    if(cur == tag) {
        /* found it. */
        if(prev) {
            prev->next = tag->next;
        } else {
            /* at the head of the list. */
            list->head = tag->next;
        }

        if(list->tail == tag) {
            list->tail = prev;
        }

        rc = PLCTAG_STATUS_OK;
    } else {
        rc = PLCTAG_STATUS_PENDING;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}


/****** Tag Control Functions ******/

/* These must all be called with the API mutex on the tag held. */

int mb_abort(plc_tag_p p_tag)
{
    modbus_tag_p tag = (modbus_tag_p)p_tag;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN, "Null tag pointer!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /*
     * This is safe to do because we hold the tag
     * API mutex here.   When the PLC thread runs
     * and calls tickle_tag (the only place where
     * the op changes) it holds the API mutex as well.
     * Thus this code below is only accessible by
     * one thread at a time.
     */
    tag->seq_id = 0;
    tag->request_num = 0;
    tag->status = (int8_t)PLCTAG_STATUS_OK;
    tag->op = TAG_OP_IDLE;

    clear_request_slot(tag->plc, tag);

    /* wake the PLC loop if we need to. */
    wake_plc_thread(tag->plc);

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}



int mb_read_start(plc_tag_p p_tag)
{
    modbus_tag_p tag = (modbus_tag_p)p_tag;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN, "Null tag pointer!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /*
     * This is safe to do because we hold the tag
     * API mutex here.   When the PLC thread runs
     * and calls tickle_tag (the only place where
     * the op changes) it holds the API mutex as well.
     * Thus this code below is only accessible by
     * one thread at a time.
     */
    if(tag->op == TAG_OP_IDLE) {
        tag->op = TAG_OP_READ_REQUEST;
    } else {
        pdebug(DEBUG_WARN, "Operation in progress!");
        return PLCTAG_ERR_BUSY;
    }

    /* wake the PLC loop if we need to. */
    wake_plc_thread(tag->plc);

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_PENDING;
}



int mb_tag_status(plc_tag_p p_tag)
{
    modbus_tag_p tag = (modbus_tag_p)p_tag;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN, "Null tag pointer!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(tag->status != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_DETAIL, "Status not OK, returning %s.", plc_tag_decode_error(tag->status));
        return tag->status;
    }

    if(tag->op != TAG_OP_IDLE) {
        pdebug(DEBUG_DETAIL, "Operation in progress, returning PLCTAG_STATUS_PENDING.");
        return PLCTAG_STATUS_PENDING;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}



/* not used. */
int mb_tickler(plc_tag_p p_tag)
{
    (void)p_tag;

    return PLCTAG_STATUS_OK;
}




int mb_write_start(plc_tag_p p_tag)
{
    modbus_tag_p tag = (modbus_tag_p)p_tag;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN, "Null tag pointer!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /*
     * This is safe to do because we hold the tag
     * API mutex here.   When the PLC thread runs
     * and calls tickle_tag (the only place where
     * the op changes) it holds the API mutex as well.
     * Thus this code below is only accessible by
     * one thread at a time.
     */
    if(tag->op == TAG_OP_IDLE) {
        tag->op = TAG_OP_WRITE_REQUEST;
    } else {
        pdebug(DEBUG_WARN, "Operation in progress!");
        return PLCTAG_ERR_BUSY;
    }

    /* wake the PLC loop if we need to. */
    wake_plc_thread(tag->plc);

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_PENDING;
}



int mb_wake_plc(plc_tag_p p_tag)
{
    modbus_tag_p tag = (modbus_tag_p)p_tag;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN, "Null tag pointer!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* wake the PLC thread. */
    wake_plc_thread(tag->plc);

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_PENDING;
}


/****** Data Accessor Functions ******/

int mb_get_int_attrib(plc_tag_p raw_tag, const char *attrib_name, int default_value)
{
    int res = default_value;
    modbus_tag_p tag = (modbus_tag_p)raw_tag;

    pdebug(DEBUG_SPEW, "Starting.");

    tag->status = PLCTAG_STATUS_OK;

    /* match the attribute. */
    if(str_cmp_i(attrib_name, "elem_size") == 0) {
        res = (tag->elem_size + 7)/8; /* return size in bytes! */
    } else if(str_cmp_i(attrib_name, "elem_count") == 0) {
        res = tag->elem_count;
    } else {
        pdebug(DEBUG_WARN,"Attribute \"%s\" is not supported.", attrib_name);
        tag->status = PLCTAG_ERR_UNSUPPORTED;
    }

    return res;
}


int mb_set_int_attrib(plc_tag_p raw_tag, const char *attrib_name, int new_value)
{
    (void)attrib_name;
    (void)new_value;

    pdebug(DEBUG_WARN, "Attribute \"%s\" is unsupported!", attrib_name);

    raw_tag->status = PLCTAG_ERR_UNSUPPORTED;

    return PLCTAG_ERR_UNSUPPORTED;
}





/****** Library level functions. *******/

void mb_teardown(void)
{
    pdebug(DEBUG_INFO, "Starting.");

    mb_library_terminating = 1;

    if(mb_mutex) {
        pdebug(DEBUG_DETAIL, "Waiting for all Modbus PLCs to terminate.");

        while(1) {
            int plcs_remain = 0;
            
            critical_block(mb_mutex) {
                plcs_remain = plcs ? 1 : 0;
            }

            if(plcs_remain) {
                sleep_ms(10); // MAGIC
            } else {
                break;
            }
        }

        pdebug(DEBUG_DETAIL, "All Modbus PLCs terminated.");
    }

    if(mb_mutex) {
        pdebug(DEBUG_DETAIL, "Destroying Modbus mutex.");
        mutex_destroy(&mb_mutex);
        mb_mutex = NULL;
    }
    pdebug(DEBUG_DETAIL, "Modbus mutex destroyed.");

    pdebug(DEBUG_INFO, "Done.");
}



int mb_init()
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    pdebug(DEBUG_DETAIL, "Setting up mutex.");
    if(!mb_mutex) {
        rc = mutex_create(&mb_mutex);
        if(rc != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Error %s creating mutex!", plc_tag_decode_error(rc));
            return rc;
        }
    }

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}

#endif // __PROTOCOLS_MB_MODBUS_C__


#ifndef __PROTOCOLS_SYSTEM_SYSTEM_C__
#define __PROTOCOLS_SYSTEM_SYSTEM_C__

static void system_tag_destroy(plc_tag_p tag);


static int system_tag_abort(plc_tag_p tag);
static int system_tag_read(plc_tag_p tag);
static int system_tag_status(plc_tag_p tag);
static int system_tag_write(plc_tag_p tag);

struct tag_vtable_t system_tag_vtable = {
    /* abort */     system_tag_abort,
    /* read */      system_tag_read,
    /* status */    system_tag_status,
    /* tickler */   NULL,
    /* write */     system_tag_write,
    /* wake_plc */  (tag_vtable_func)NULL,

    /* data accessors */

    /* get_int_attrib */ NULL,
    /* set_int_attrib */ NULL

};

tag_byte_order_t system_tag_byte_order = {
    .is_allocated = 0,

    .str_is_defined = 1,
    .str_is_counted = 0,
    .str_is_fixed_length = 0,
    .str_is_zero_terminated = 1, /* C-style string. */
    .str_is_byte_swapped = 0,

    .str_count_word_bytes = 0,
    .str_max_capacity = 0,
    .str_total_length = 0,
    .str_pad_bytes = 0,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},    
};



plc_tag_p system_tag_create(attr attribs, void (*tag_callback_func)(int32_t tag_id, int event, int status, void *userdata), void *userdata)
{
    int rc = PLCTAG_STATUS_OK;
    system_tag_p tag = NULL;
    const char *name = attr_get_str(attribs, "name", NULL);

    pdebug(DEBUG_INFO,"Starting.");

    /* check the name, if none given, punt. */
    if(!name || str_length(name) < 1) {
        pdebug(DEBUG_ERROR, "System tag name is empty or missing!");
        return PLC_TAG_P_NULL;
    }

    pdebug(DEBUG_DETAIL,"Creating special tag %s", name);

    /*
     * allocate memory for the new tag.  Do this first so that
     * we have a vehicle for returning status.
     */

    tag = (system_tag_p)rc_alloc(sizeof(struct system_tag_t), (rc_cleanup_func)system_tag_destroy);

    if(!tag) {
        pdebug(DEBUG_ERROR,"Unable to allocate memory for system tag!");
        return PLC_TAG_P_NULL;
    }

    /*
     * we got far enough to allocate memory, set the default vtable up
     * in case we need to abort later.
     */
    tag->vtable = &system_tag_vtable;

    /* set up the generic parts. */
    rc = plc_tag_generic_init_tag((plc_tag_p)tag, attribs, tag_callback_func, userdata);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to initialize generic tag parts!");
        rc_dec(tag);
        return (plc_tag_p)NULL;
    }

    /* set the byte order. */
    tag->byte_order = &system_tag_byte_order;

    /* get the name and copy it */
    str_copy(tag->name, MAX_SYSTEM_TAG_NAME - 1, name);
    tag->name[MAX_SYSTEM_TAG_NAME - 1] = '\0';

    /* point data at the backing store. */
    tag->data = &tag->backing_data[0];
    tag->size = (int)sizeof(tag->backing_data);

    pdebug(DEBUG_INFO,"Done");

    return (plc_tag_p)tag;
}


static int system_tag_abort(plc_tag_p tag)
{
    /* there are no outstanding operations, so everything is OK. */
    tag->status = PLCTAG_STATUS_OK;
    return PLCTAG_STATUS_OK;
}



static void system_tag_destroy(plc_tag_p ptag)
{
    system_tag_p tag = (system_tag_p)ptag;

    if(!tag) {
        return;
    }

    if(ptag->ext_mutex) {
        mutex_destroy(&ptag->ext_mutex);
    }

    if(ptag->api_mutex) {
        mutex_destroy(&ptag->api_mutex);
    }

    if(ptag->tag_cond_wait) {
        cond_destroy(&ptag->tag_cond_wait);
    }

    if(tag->byte_order && tag->byte_order->is_allocated) {
        mem_free(tag->byte_order);
        tag->byte_order = NULL;
    }

    return;
}


static int system_tag_read(plc_tag_p ptag)
{
    system_tag_p tag = (system_tag_p)ptag;
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO,"Starting.");

    if(!tag) {
        return PLCTAG_ERR_NULL_PTR;
    }

    if(str_cmp_i(&tag->name[0],"version") == 0) {
        pdebug(DEBUG_DETAIL,"Version is %s",VERSION);
        str_copy((char *)(&tag->data[0]), MAX_SYSTEM_TAG_SIZE , VERSION);
        tag->data[str_length(VERSION)] = 0;
        rc = PLCTAG_STATUS_OK;
    } else if(str_cmp_i(&tag->name[0],"debug") == 0) {
        int debug_level = get_debug_level();
        tag->data[0] = (uint8_t)(debug_level & 0xFF);
        tag->data[1] = (uint8_t)((debug_level >> 8) & 0xFF);
        tag->data[2] = (uint8_t)((debug_level >> 16) & 0xFF);
        tag->data[3] = (uint8_t)((debug_level >> 24) & 0xFF);
        rc = PLCTAG_STATUS_OK;
    } else {
        pdebug(DEBUG_WARN, "Unsupported system tag %s!", tag->name);
        rc = PLCTAG_ERR_UNSUPPORTED;
    }

    /* safe here because we are still within the API mutex. */
    tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_READ_STARTED, PLCTAG_STATUS_OK);
    tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_READ_COMPLETED, PLCTAG_STATUS_OK);
    plc_tag_generic_handle_event_callbacks((plc_tag_p)tag);

    pdebug(DEBUG_INFO,"Done.");

    return rc;
}


static int system_tag_status(plc_tag_p tag)
{
    tag->status = PLCTAG_STATUS_OK;
    return PLCTAG_STATUS_OK;
}


static int system_tag_write(plc_tag_p ptag)
{
    int rc = PLCTAG_STATUS_OK;
    system_tag_p tag = (system_tag_p)ptag;

    if(!tag) {
        return PLCTAG_ERR_NULL_PTR;
    }

    /* raise this here so that the callback can update the tag buffer. */
    tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_WRITE_STARTED, PLCTAG_STATUS_PENDING);
    plc_tag_generic_handle_event_callbacks((plc_tag_p)tag);

    /* the version is static */
    if(str_cmp_i(&tag->name[0],"debug") == 0) {
        int res = 0;
        res = (int32_t)(((uint32_t)(tag->data[0])) +
                        ((uint32_t)(tag->data[1]) << 8) +
                        ((uint32_t)(tag->data[2]) << 16) +
                        ((uint32_t)(tag->data[3]) << 24));
        set_debug_level(res);
        rc = PLCTAG_STATUS_OK;
    } else if(str_cmp_i(&tag->name[0],"version") == 0) {
        rc = PLCTAG_ERR_NOT_IMPLEMENTED;
    } else {
        pdebug(DEBUG_WARN, "Unsupported system tag %s!", tag->name);
        rc = PLCTAG_ERR_UNSUPPORTED;
    }

    tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_WRITE_COMPLETED, PLCTAG_STATUS_OK);
    plc_tag_generic_handle_event_callbacks((plc_tag_p)tag);

    pdebug(DEBUG_INFO,"Done.");

    return rc;
}

#endif // __PROTOCOLS_SYSTEM_SYSTEM_C__


#ifndef __PROTOCOLS_AB_AB_COMMON_C__
#define __PROTOCOLS_AB_AB_COMMON_C__

#include <ctype.h>
#include <limits.h>
#include <float.h>

/*
 * Externally visible global variables
 */

//volatile ab_session_p sessions = NULL;
//volatile mutex_p global_session_mut = NULL;
//
//volatile vector_p read_group_tags = NULL;


/* request/response handling thread */
volatile thread_p io_handler_thread = NULL;

volatile int ab_protocol_terminating = 0;



/*
 * Generic Rockwell/Allen-Bradley protocol functions.
 *
 * These are the primary entry points into the AB protocol
 * stack.
 */


#define DEFAULT_NUM_RETRIES (5)
#define DEFAULT_RETRY_INTERVAL (300)


/* forward declarations*/
static int get_tag_data_type(ab_tag_p tag, attr attribs);

static void ab_tag_destroy(ab_tag_p tag);
static int default_abort(plc_tag_p tag);
static int default_read(plc_tag_p tag);
static int default_status(plc_tag_p tag);
static int default_tickler(plc_tag_p tag);
static int default_write(plc_tag_p tag);


/* vtables for different kinds of tags */
struct tag_vtable_t default_vtable = {
    default_abort,
    default_read,
    default_status,
    default_tickler,
    default_write,
    (tag_vtable_func)NULL, /* this is not portable! */

    /* attribute accessors */
    ab_get_int_attrib,
    ab_set_int_attrib
};


/*
 * Public functions.
 */


int ab_init(void)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO,"Initializing AB protocol library.");

    ab_protocol_terminating = 0;

    if((rc = session_startup()) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to initialize session library!");
        return rc;
    }

    pdebug(DEBUG_INFO,"Finished initializing AB protocol library.");

    return rc;
}

/*
 * called when the whole program is going to terminate.
 */
void ab_teardown(void)
{
    pdebug(DEBUG_INFO,"Releasing global AB protocol resources.");

    if(io_handler_thread) {
        pdebug(DEBUG_INFO,"Terminating IO thread.");
        /* signal the IO thread to quit first. */
        ab_protocol_terminating = 1;

        /* wait for the thread to die */
        thread_join(io_handler_thread);
        thread_destroy((thread_p*)&io_handler_thread);
    } else {
        pdebug(DEBUG_INFO, "IO thread already stopped.");
    }

    pdebug(DEBUG_INFO,"Freeing session information.");

    session_teardown();

    ab_protocol_terminating = 0;

    pdebug(DEBUG_INFO,"Done.");
}



plc_tag_p ab_tag_create(attr attribs, void (*tag_callback_func)(int32_t tag_id, int event, int status, void *userdata), void *userdata)
{
    ab_tag_p tag = AB_TAG_NULL;
    const char *path = NULL;
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO,"Starting.");

    /*
     * allocate memory for the new tag.  Do this first so that
     * we have a vehicle for returning status.
     */

    tag = (ab_tag_p)rc_alloc(sizeof(struct ab_tag_t), (rc_cleanup_func)ab_tag_destroy);
    if(!tag) {
        pdebug(DEBUG_ERROR,"Unable to allocate memory for AB EIP tag!");
        return (plc_tag_p)NULL;
    }

    pdebug(DEBUG_DETAIL, "tag=%p", tag);

    /*
     * we got far enough to allocate memory, set the default vtable up
     * in case we need to abort later.
     */

    tag->vtable = &default_vtable;

    /* set up the generic parts. */
    rc = plc_tag_generic_init_tag((plc_tag_p)tag, attribs, tag_callback_func, userdata);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to initialize generic tag parts!");
        rc_dec(tag);
        return (plc_tag_p)NULL;
    }

    /*
     * check the CPU type.
     *
     * This determines the protocol type.
     */

    if(check_cpu(tag, attribs) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"CPU type not valid or missing.");
        /* tag->status = PLCTAG_ERR_BAD_DEVICE; */
        rc_dec(tag);
        return (plc_tag_p)NULL;
    }

    /* set up any required settings based on the cpu type. */
    switch(tag->plc_type) {
    case AB_PLC_PLC5:
        tag->use_connected_msg = 0;
        tag->allow_packing = 0;
        break;

    case AB_PLC_SLC:
        tag->use_connected_msg = 0;
        tag->allow_packing = 0;
        break;

    case AB_PLC_MLGX:
        tag->use_connected_msg = 0;
        tag->allow_packing = 0;
        break;

    case AB_PLC_LGX_PCCC:
        tag->use_connected_msg = 0;
        tag->allow_packing = 0;
        break;

    case AB_PLC_LGX:
        /* default to requiring a connection and allowing packing. */
        tag->use_connected_msg = attr_get_int(attribs,"use_connected_msg", 1);
        tag->allow_packing = attr_get_int(attribs, "allow_packing", 1);
        break;

    case AB_PLC_MICRO800:
        /* we must use connected messaging here. */
        pdebug(DEBUG_DETAIL, "Micro800 needs connected messaging.");
        tag->use_connected_msg = 1;

        /* Micro800 cannot pack requests. */
        tag->allow_packing = 0;
        break;

    case AB_PLC_OMRON_NJNX:
        tag->use_connected_msg = 1;

        /*
         * Default packing to off.  Omron requires the client to do the calculation of
         * whether the results will fit or not.
         */
        tag->allow_packing = attr_get_int(attribs, "allow_packing", 0);
        break;

    default:
        pdebug(DEBUG_WARN, "Unknown PLC type!");
        tag->status = PLCTAG_ERR_BAD_CONFIG;
        return (plc_tag_p)tag;
        break;
    }

    /* make sure that the connection requirement is forced. */
    attr_set_int(attribs, "use_connected_msg", tag->use_connected_msg);

    /* get the connection path.  We need this to make a decision about the PLC. */
    path = attr_get_str(attribs,"path",NULL);

    /*
     * Find or create a session.
     *
     * All tags need sessions.  They are the TCP connection to the gateway PLC.
     */
    if(session_find_or_create(&tag->session, attribs) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_INFO,"Unable to create session!");
        tag->status = PLCTAG_ERR_BAD_GATEWAY;
        return (plc_tag_p)tag;
    }

    pdebug(DEBUG_DETAIL, "using session=%p", tag->session);

    /* get the tag data type, or try. */
    rc = get_tag_data_type(tag, attribs);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Error %s getting tag element data type or handling special tag!", plc_tag_decode_error(rc));
        tag->status = (int8_t)rc;
        return (plc_tag_p)tag;
    }

    /* set up PLC-specific information. */
    switch(tag->plc_type) {
    case AB_PLC_PLC5:
        if(!tag->session->is_dhp) {
            pdebug(DEBUG_DETAIL, "Setting up PLC/5 tag.");

            if(str_length(path)) {
                pdebug(DEBUG_WARN, "A path is not supported for this PLC type if it is not for a DH+ bridge.");
            }

            tag->use_connected_msg = 0;
            tag->vtable = &plc5_vtable;
        } else {
            pdebug(DEBUG_DETAIL, "Setting up PLC/5 via DH+ bridge tag.");
            tag->use_connected_msg = 1;
            tag->vtable = &eip_plc5_dhp_vtable;
        }

        tag->byte_order = &plc5_tag_byte_order;

        tag->allow_packing = 0;
        break;

    case AB_PLC_SLC:
    case AB_PLC_MLGX:
        if(!tag->session->is_dhp) {

            if(str_length(path)) {
                pdebug(DEBUG_WARN, "A path is not supported for this PLC type if it is not for a DH+ bridge.");
            }

            pdebug(DEBUG_DETAIL, "Setting up SLC/MicroLogix tag.");
            tag->use_connected_msg = 0;
            tag->vtable = &slc_vtable;
        } else {
            pdebug(DEBUG_DETAIL, "Setting up SLC/MicroLogix via DH+ bridge tag.");
            tag->use_connected_msg = 1;
            tag->vtable = &eip_slc_dhp_vtable;
        }

        tag->byte_order = &slc_tag_byte_order;

        tag->allow_packing = 0;
        break;

    case AB_PLC_LGX_PCCC:
        pdebug(DEBUG_DETAIL, "Setting up PCCC-mapped Logix tag.");
        tag->use_connected_msg = 0;
        tag->allow_packing = 0;
        tag->vtable = &lgx_pccc_vtable;

        tag->byte_order = &slc_tag_byte_order;

        break;

    case AB_PLC_LGX:
        pdebug(DEBUG_DETAIL, "Setting up Logix tag.");

        /* Logix tags need a path. */
        if(path == NULL && tag->plc_type == AB_PLC_LGX) {
            pdebug(DEBUG_WARN,"A path is required for Logix-class PLCs!");
            tag->status = PLCTAG_ERR_BAD_PARAM;
            return (plc_tag_p)tag;
        }

        /* if we did not fill in the byte order elsewhere, fill it in now. */
        if(!tag->byte_order) {
            pdebug(DEBUG_DETAIL, "Using default Logix byte order.");
            tag->byte_order = &logix_tag_byte_order;
        }

        /* if this was not filled in elsewhere default to Logix */
        if(tag->vtable == &default_vtable || !tag->vtable) {
            pdebug(DEBUG_DETAIL, "Setting default Logix vtable.");
            tag->vtable = &eip_cip_vtable;
        }

        /* default to requiring a connection. */
        tag->use_connected_msg = attr_get_int(attribs,"use_connected_msg", 1);
        tag->allow_packing = attr_get_int(attribs, "allow_packing", 1);

        break;

    case AB_PLC_MICRO800:
        pdebug(DEBUG_DETAIL, "Setting up Micro8X0 tag.");

        if(path || str_length(path)) {
            pdebug(DEBUG_WARN, "A path is not supported for this PLC type.");
        }

        /* if we did not fill in the byte order elsewhere, fill it in now. */
        if(!tag->byte_order) {
            pdebug(DEBUG_DETAIL, "Using default Micro8x0 byte order.");
            tag->byte_order = &logix_tag_byte_order;
        }

        /* if this was not filled in elsewhere default to generic *Logix */
        if(tag->vtable == &default_vtable || !tag->vtable) {
            pdebug(DEBUG_DETAIL, "Setting default Logix vtable.");
            tag->vtable = &eip_cip_vtable;
        }

        tag->use_connected_msg = 1;
        tag->allow_packing = 0;

        break;

    case AB_PLC_OMRON_NJNX:
        pdebug(DEBUG_DETAIL, "Setting up OMRON NJ/NX Series tag.");

        if(str_length(path) == 0) {
            pdebug(DEBUG_WARN,"A path is required for this PLC type.");
            tag->status = PLCTAG_ERR_BAD_PARAM;
            return (plc_tag_p)tag;
        }

        /* if we did not fill in the byte order elsewhere, fill it in now. */
        if(!tag->byte_order) {
            pdebug(DEBUG_DETAIL, "Using default Omron byte order.");
            tag->byte_order = &omron_njnx_tag_byte_order;
        }

        /* if this was not filled in elsewhere default to generic *Logix */
        if(tag->vtable == &default_vtable || !tag->vtable) {
            pdebug(DEBUG_DETAIL, "Setting default Logix vtable.");
            tag->vtable = &eip_cip_vtable;
        }

        tag->use_connected_msg = 1;
        tag->allow_packing = attr_get_int(attribs, "allow_packing", 0);

        break;

    default:
        pdebug(DEBUG_WARN, "Unknown PLC type!");
        tag->status = PLCTAG_ERR_BAD_CONFIG;
        return (plc_tag_p)tag;
    }

    /* pass the connection requirement since it may be overridden above. */
    attr_set_int(attribs, "use_connected_msg", tag->use_connected_msg);

    /* get the element count, default to 1 if missing. */
    tag->elem_count = attr_get_int(attribs,"elem_count", 1);

    switch(tag->plc_type) {
    case AB_PLC_OMRON_NJNX:
        if (tag->elem_count != 1) {
            tag->elem_count = 1;
            pdebug(DEBUG_WARN,"Attribute elem_count should be 1!");
        }

        /* from here is the same as a AB_PLC_MICRO800. */

        /* fall through */
    case AB_PLC_LGX:
    case AB_PLC_MICRO800:
        /* fill this in when we read the tag. */
        //tag->elem_size = 0;
        tag->size = 0;
        tag->data = NULL;
        break;

    default:
        /* we still need size on non Logix-class PLCs */
        /* get the element size if it is not already set. */
        if(!tag->elem_size) {
            tag->elem_size = attr_get_int(attribs, "elem_size", 0);
        }

        /* Determine the tag size */
        tag->size = (tag->elem_count) * (tag->elem_size);
        if(tag->size == 0) {
            /* failure! Need data_size! */
            pdebug(DEBUG_WARN,"Tag size is zero!");
            tag->status = PLCTAG_ERR_BAD_PARAM;
            return (plc_tag_p)tag;
        }

        /* this may be changed in the future if this is a tag list request. */
        tag->data = (uint8_t*)mem_alloc(tag->size);

        if(tag->data == NULL) {
            pdebug(DEBUG_WARN,"Unable to allocate tag data!");
            tag->status = PLCTAG_ERR_NO_MEM;
            return (plc_tag_p)tag;
        }
        break;
    }

    /*
     * check the tag name, this is protocol specific.
     */

    if(!tag->special_tag && check_tag_name(tag, attr_get_str(attribs,"name",NULL)) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_INFO,"Bad tag name!");
        tag->status = PLCTAG_ERR_BAD_PARAM;
        return (plc_tag_p)tag;
    }

    /* kick off a read to get the tag type and size. */
    if(!tag->special_tag && tag->vtable->read) {
        /* trigger the first read. */
        pdebug(DEBUG_DETAIL, "Kicking off initial read.");

        tag->first_read = 1;
        tag->read_in_flight = 1;
        tag->vtable->read((plc_tag_p)tag);
        //tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_READ_STARTED, tag->status);
    } else {
        pdebug(DEBUG_DETAIL, "Not kicking off initial read: tag is special or does not have read function.");

        /* force the created event because we do not do an initial read here. */
        tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_CREATED, tag->status);
    }

    pdebug(DEBUG_DETAIL, "Using vtable %p.", tag->vtable);

    pdebug(DEBUG_INFO,"Done.");

    return (plc_tag_p)tag;
}


/*
 * determine the tag's data type and size.  Or at least guess it.
 */

int get_tag_data_type(ab_tag_p tag, attr attribs)
{
    int rc = PLCTAG_STATUS_OK;
    const char *elem_type = NULL;
    const char *tag_name = NULL;
    pccc_addr_t file_addr =  {};

    pdebug(DEBUG_DETAIL, "Starting.");

    switch(tag->plc_type) {
    case AB_PLC_PLC5:
    case AB_PLC_SLC:
    case AB_PLC_LGX_PCCC:
    case AB_PLC_MLGX:
        tag_name = attr_get_str(attribs,"name", NULL);

        rc = parse_pccc_logical_address(tag_name, &file_addr);
        if(rc != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Unable to parse data file %s!", tag_name);
            return rc;
        }

        tag->elem_size = file_addr.element_size_bytes;
        tag->file_type = file_addr.file_type;

        break;

    case AB_PLC_LGX:
    case AB_PLC_MICRO800:
    case AB_PLC_OMRON_NJNX:
        /* look for the elem_type attribute. */
        elem_type = attr_get_str(attribs, "elem_type", NULL);
        if(elem_type) {
            if(str_cmp_i(elem_type,"lint") == 0 || str_cmp_i(elem_type, "ulint") == 0) {
                pdebug(DEBUG_DETAIL,"Found tag element type of 64-bit integer.");
                tag->elem_size = 8;
                tag->elem_type = AB_TYPE_INT64;
            } else if(str_cmp_i(elem_type,"dint") == 0 || str_cmp_i(elem_type,"udint") == 0) {
                pdebug(DEBUG_DETAIL,"Found tag element type of 32-bit integer.");
                tag->elem_size = 4;
                tag->elem_type = AB_TYPE_INT32;
            } else if(str_cmp_i(elem_type,"int") == 0 || str_cmp_i(elem_type,"uint") == 0) {
                pdebug(DEBUG_DETAIL,"Found tag element type of 16-bit integer.");
                tag->elem_size = 2;
                tag->elem_type = AB_TYPE_INT16;
            } else if(str_cmp_i(elem_type,"sint") == 0 || str_cmp_i(elem_type,"usint") == 0) {
                pdebug(DEBUG_DETAIL,"Found tag element type of 8-bit integer.");
                tag->elem_size = 1;
                tag->elem_type = AB_TYPE_INT8;
            } else if(str_cmp_i(elem_type,"bool") == 0) {
                pdebug(DEBUG_DETAIL,"Found tag element type of bit.");
                tag->elem_size = 1;
                tag->elem_type = AB_TYPE_BOOL;
            } else if(str_cmp_i(elem_type,"bool array") == 0) {
                pdebug(DEBUG_DETAIL,"Found tag element type of bool array.");
                tag->elem_size = 4;
                tag->elem_type = AB_TYPE_BOOL_ARRAY;
            } else if(str_cmp_i(elem_type,"real") == 0) {
                pdebug(DEBUG_DETAIL,"Found tag element type of 32-bit float.");
                tag->elem_size = 4;
                tag->elem_type = AB_TYPE_FLOAT32;
            } else if(str_cmp_i(elem_type,"lreal") == 0) {
                pdebug(DEBUG_DETAIL,"Found tag element type of 64-bit float.");
                tag->elem_size = 8;
                tag->elem_type = AB_TYPE_FLOAT64;
            } else if(str_cmp_i(elem_type,"string") == 0) {
                pdebug(DEBUG_DETAIL,"Fount tag element type of string.");
                tag->elem_size = 88;
                tag->elem_type = AB_TYPE_STRING;
            } else if(str_cmp_i(elem_type,"short string") == 0) {
                pdebug(DEBUG_DETAIL,"Found tag element type of short string.");
                tag->elem_size = 256; /* TODO - find the real length */
                tag->elem_type = AB_TYPE_SHORT_STRING;
            } else {
                pdebug(DEBUG_DETAIL, "Unknown tag type %s", elem_type);
                return PLCTAG_ERR_UNSUPPORTED;
            }
        } else {
            /*
             * We have two cases
             *      * tag listing, but only for CIP PLCs (but not for UDTs!).
             *      * no type, just elem_size.
             * Otherwise this is an error.
             */
            int elem_size = attr_get_int(attribs, "elem_size", 0);
            int cip_plc = !!(tag->plc_type == AB_PLC_LGX || tag->plc_type == AB_PLC_MICRO800 || tag->plc_type == AB_PLC_OMRON_NJNX);

            if(cip_plc) {
                const char *tmp_tag_name = attr_get_str(attribs, "name", NULL);
                int special_tag_rc = PLCTAG_STATUS_OK;

                /* check for special tags. */
                if(str_cmp_i(tmp_tag_name, "@raw") == 0) {
                    special_tag_rc = setup_raw_tag(tag);
                } else if(str_str_cmp_i(tmp_tag_name, "@tags")) {
                    // if(tag->plc_type != AB_PLC_OMRON_NJNX) {
                        special_tag_rc = setup_tag_listing_tag(tag, tmp_tag_name);
                    // } else {
                    //     pdebug(DEBUG_WARN, "Tag listing is not supported for Omron PLCs.");
                    //     special_tag_rc = PLCTAG_ERR_UNSUPPORTED;
                    // }
                } else if(str_str_cmp_i(tmp_tag_name, "@udt/")) {
                    // if(tag->plc_type != AB_PLC_OMRON_NJNX) {
                        /* only supported on *Logix */
                        special_tag_rc = setup_udt_tag(tag, tmp_tag_name);
                    // } else {
                    //     pdebug(DEBUG_WARN, "UDT listing is not supported for Omron PLCs.");
                    //     special_tag_rc = PLCTAG_ERR_UNSUPPORTED;
                    // }
                } /* else not a special tag. */

                if(special_tag_rc != PLCTAG_STATUS_OK) {
                    pdebug(DEBUG_WARN, "Error parsing tag listing name!");
                    return special_tag_rc;
                }
            }

            /* if we did not set an element size yet, set one. */
            if(tag->elem_size == 0) {
                if(elem_size > 0) {
                    pdebug(DEBUG_INFO, "Setting element size to %d.", elem_size);
                    tag->elem_size = elem_size;
                }
            } else {
                if(elem_size > 0) {
                    pdebug(DEBUG_WARN, "Tag has elem_size and either is a tag listing or has elem_type, only use one!");
                }
            }
        }

        break;

    default:
        pdebug(DEBUG_WARN, "Unknown PLC type!");
        return PLCTAG_ERR_BAD_DEVICE;
        break;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}


int default_abort(plc_tag_p tag)
{
    (void)tag;

    pdebug(DEBUG_WARN, "This should be overridden by a PLC-specific function!");

    return PLCTAG_ERR_NOT_IMPLEMENTED;
}


int default_read(plc_tag_p tag)
{
    (void)tag;

    pdebug(DEBUG_WARN, "This should be overridden by a PLC-specific function!");

    return PLCTAG_ERR_NOT_IMPLEMENTED;
}

int default_status(plc_tag_p tag)
{
    pdebug(DEBUG_WARN, "This should be overridden by a PLC-specific function!");

    if(tag) {
        return tag->status;
    } else {
        return PLCTAG_ERR_NOT_FOUND;
    }
}


int default_tickler(plc_tag_p tag)
{
    (void)tag;

    pdebug(DEBUG_WARN, "This should be overridden by a PLC-specific function!");

    return PLCTAG_STATUS_OK;
}



int default_write(plc_tag_p tag)
{
    (void)tag;

    pdebug(DEBUG_WARN, "This should be overridden by a PLC-specific function!");

    return PLCTAG_ERR_NOT_IMPLEMENTED;
}



/*
* ab_tag_abort
*
* This does the work of stopping any inflight requests.
* This is not thread-safe.  It must be called from a function
* that locks the tag's mutex or only from a single thread.
*/

int ab_tag_abort(ab_tag_p tag)
{
    pdebug(DEBUG_DETAIL, "Starting.");

    if(tag->req) {
        spin_block(&tag->req->lock) {
            tag->req->abort_request = 1;
        }

        tag->req = (ab_request_p)rc_dec(tag->req);
    } else {
        pdebug(DEBUG_DETAIL, "Called without a request in flight.");
    }

    tag->read_in_progress = 0;
    tag->write_in_progress = 0;
    tag->offset = 0;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}




/*
 * ab_tag_status
 *
 * Generic status checker.   May be overridden by individual PLC types.
 */
int ab_tag_status(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    if (tag->read_in_progress) {
        return PLCTAG_STATUS_PENDING;
    }

    if (tag->write_in_progress) {
        return PLCTAG_STATUS_PENDING;
    }

    if(tag->session) {
        rc = tag->status;
    } else {
        /* this is not OK.  This is fatal! */
        rc = PLCTAG_ERR_CREATE;
    }

    return rc;
}






/*
 * ab_tag_destroy
 *
 * This blocks on the global library mutex.  This should
 * be fixed to allow for more parallelism.  For now, safety is
 * the primary concern.
 */

void ab_tag_destroy(ab_tag_p tag)
{
    ab_session_p session = NULL;

    pdebug(DEBUG_INFO, "Starting.");

    /* already destroyed? */
    if (!tag) {
        pdebug(DEBUG_WARN,"Tag pointer is null!");

        return;
    }

    /* abort anything in flight */
    ab_tag_abort(tag);

    session = tag->session;

    /* tags should always have a session.  Release it. */
    pdebug(DEBUG_DETAIL,"Getting ready to release tag session %p",tag->session);
    if(session) {
        pdebug(DEBUG_DETAIL, "Removing tag from session.");
        rc_dec(session);
        tag->session = NULL;
    } else {
        pdebug(DEBUG_WARN,"No session pointer!");
    }

    if(tag->ext_mutex) {
        mutex_destroy(&(tag->ext_mutex));
        tag->ext_mutex = NULL;
    }

    if(tag->api_mutex) {
        mutex_destroy(&(tag->api_mutex));
        tag->api_mutex = NULL;
    }

    if(tag->tag_cond_wait) {
        cond_destroy(&(tag->tag_cond_wait));
        tag->tag_cond_wait = NULL;
    }

    if(tag->byte_order && tag->byte_order->is_allocated) {
        mem_free(tag->byte_order);
        tag->byte_order = NULL;
    }

    if (tag->data) {
        mem_free(tag->data);
        tag->data = NULL;
    }

    pdebug(DEBUG_INFO,"Finished releasing all tag resources.");

    pdebug(DEBUG_INFO, "done");
}


int ab_get_int_attrib(plc_tag_p raw_tag, const char *attrib_name, int default_value)
{
    int res = default_value;
    ab_tag_p tag = (ab_tag_p)raw_tag;

    pdebug(DEBUG_SPEW, "Starting.");

    /* assume we have a match. */
    tag->status = PLCTAG_STATUS_OK;

    /* match the attribute. */
    if(str_cmp_i(attrib_name, "elem_size") == 0) {
        res = tag->elem_size;
    } else if(str_cmp_i(attrib_name, "elem_count") == 0) {
        res = tag->elem_count;
    } else if(str_cmp_i(attrib_name, "elem_type") == 0) {
        switch(tag->plc_type) {
            case AB_PLC_PLC5: /* fall through */
            case AB_PLC_MLGX: /* fall through */
            case AB_PLC_SLC: /* fall through */
            case AB_PLC_LGX_PCCC:
                res = (int)(tag->file_type);
                break;
            case AB_PLC_LGX: /* fall through */
            case AB_PLC_MICRO800: /* fall through */
            case AB_PLC_OMRON_NJNX:
                res = (int)(tag->elem_type);
                break;
            default: 
                pdebug(DEBUG_WARN, "Unsupported PLC type %d!", tag->plc_type);
                break;
        }
    } else {
        pdebug(DEBUG_WARN, "Unsupported attribute name \"%s\"!", attrib_name);
        tag->status = PLCTAG_ERR_UNSUPPORTED;
    }

    return res;
}


int ab_set_int_attrib(plc_tag_p raw_tag, const char *attrib_name, int new_value)
{
    (void)attrib_name;
    (void)new_value;

    pdebug(DEBUG_WARN, "Unsupported attribute \"%s\"!", attrib_name);

    raw_tag->status  = PLCTAG_ERR_UNSUPPORTED;

    return PLCTAG_ERR_UNSUPPORTED;
}



plc_type_t get_plc_type(attr attribs)
{
    const char *cpu_type = attr_get_str(attribs, "plc", attr_get_str(attribs, "cpu", "NONE"));

    if (!str_cmp_i(cpu_type, "plc") || !str_cmp_i(cpu_type, "plc5")) {
        pdebug(DEBUG_DETAIL,"Found PLC/5 PLC.");
        return AB_PLC_PLC5;
    } else if ( !str_cmp_i(cpu_type, "slc") || !str_cmp_i(cpu_type, "slc500")) {
        pdebug(DEBUG_DETAIL,"Found SLC 500 PLC.");
        return AB_PLC_SLC;
    } else if (!str_cmp_i(cpu_type, "lgxpccc") || !str_cmp_i(cpu_type, "logixpccc") || !str_cmp_i(cpu_type, "lgxplc5") || !str_cmp_i(cpu_type, "logixplc5") ||
               !str_cmp_i(cpu_type, "lgx-pccc") || !str_cmp_i(cpu_type, "logix-pccc") || !str_cmp_i(cpu_type, "lgx-plc5") || !str_cmp_i(cpu_type, "logix-plc5")) {
        pdebug(DEBUG_DETAIL,"Found Logix-class PLC using PCCC protocol.");
        return AB_PLC_LGX_PCCC;
    } else if (!str_cmp_i(cpu_type, "micrologix800") || !str_cmp_i(cpu_type, "mlgx800") || !str_cmp_i(cpu_type, "micro800")) {
        pdebug(DEBUG_DETAIL,"Found Micro8xx PLC.");
        return AB_PLC_MICRO800;
    } else if (!str_cmp_i(cpu_type, "micrologix") || !str_cmp_i(cpu_type, "mlgx")) {
        pdebug(DEBUG_DETAIL,"Found MicroLogix PLC.");
        return AB_PLC_MLGX;
    } else if (!str_cmp_i(cpu_type, "compactlogix") || !str_cmp_i(cpu_type, "clgx") || !str_cmp_i(cpu_type, "lgx") ||
               !str_cmp_i(cpu_type, "controllogix") || !str_cmp_i(cpu_type, "contrologix") ||
               !str_cmp_i(cpu_type, "logix")) {
        pdebug(DEBUG_DETAIL,"Found ControlLogix/CompactLogix PLC.");
        return AB_PLC_LGX;
    } else if (!str_cmp_i(cpu_type, "omron-njnx") || !str_cmp_i(cpu_type, "omron-nj") || !str_cmp_i(cpu_type, "omron-nx") || !str_cmp_i(cpu_type, "njnx")
               || !str_cmp_i(cpu_type, "nx1p2")) {
        pdebug(DEBUG_DETAIL,"Found OMRON NJ/NX Series PLC.");
        return AB_PLC_OMRON_NJNX;
    } else {
        pdebug(DEBUG_WARN, "Unsupported device type: %s", cpu_type);

        return AB_PLC_NONE;
    }
}



int check_cpu(ab_tag_p tag, attr attribs)
{
    plc_type_t result = get_plc_type(attribs);

    if(result != AB_PLC_NONE) {
        tag->plc_type = result;
        return PLCTAG_STATUS_OK;
    } else {
        tag->plc_type = result;
        return PLCTAG_ERR_BAD_DEVICE;
    }
}

int check_tag_name(ab_tag_p tag, const char* name)
{
    int rc = PLCTAG_STATUS_OK;
    pccc_addr_t pccc_address;

    if (!name) {
        pdebug(DEBUG_WARN,"No tag name parameter found!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    mem_set(&pccc_address, 0, sizeof(pccc_address));

    /* attempt to parse the tag name */
    switch (tag->plc_type) {
    case AB_PLC_PLC5:
    case AB_PLC_LGX_PCCC:
        if((rc = parse_pccc_logical_address(name, &pccc_address))) {
            pdebug(DEBUG_WARN, "Parse of PCCC-style tag name %s failed!", name);
            return rc;
        }

        if(pccc_address.is_bit) {
            tag->is_bit = 1;
            tag->bit = (int)(unsigned int)pccc_address.bit;
            pdebug(DEBUG_DETAIL, "PLC/5 address references bit %d.", tag->bit);
        }

        if((rc = plc5_encode_address(tag->encoded_name, &(tag->encoded_name_size), MAX_TAG_NAME, &pccc_address)) != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Encoding of PLC/5-style tag name %s failed!", name);
            return rc;
        }

        break;

    case AB_PLC_SLC:
    case AB_PLC_MLGX:
        if((rc = parse_pccc_logical_address(name, &pccc_address))) {
            pdebug(DEBUG_WARN, "Parse of PCCC-style tag name %s failed!", name);
            return rc;
        }

        if(pccc_address.is_bit) {
            tag->is_bit = 1;
            tag->bit = (int)(unsigned int)pccc_address.bit;
            pdebug(DEBUG_DETAIL, "SLC/Micrologix address references bit %d.", tag->bit);
        }

        if ((rc = slc_encode_address(tag->encoded_name, &(tag->encoded_name_size), MAX_TAG_NAME, &pccc_address)) != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Encoding of SLC-style tag name %s failed!", name);
            return rc;
        }

        break;

    case AB_PLC_MICRO800:
    case AB_PLC_LGX:
    case AB_PLC_OMRON_NJNX:
        if ((rc = cip_encode_tag_name(tag, name)) != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "parse of CIP-style tag name %s failed!", name);

            return rc;
        }

        break;

    default:
        /* how would we get here? */
        pdebug(DEBUG_WARN, "unsupported PLC type %d", tag->plc_type);

        return PLCTAG_ERR_BAD_PARAM;

        break;
    }

    return PLCTAG_STATUS_OK;
}





/**
 * @brief Check the status of the read request
 * 
 * This function checks the request itself and updates the
 * tag if there are any failures or changes that need to be
 * made due to the request status.
 * 
 * The tag and the request must not be deleted out from underneath
 * this function.   Ideally both are held with write mutexes.
 * 
 * @return status of the request.
 * 
 */

int check_read_request_status(ab_tag_p tag, ab_request_p request)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!request) {
        tag->read_in_progress = 0;
        tag->offset = 0;

        pdebug(DEBUG_WARN,"Read in progress, but no request in flight!");

        return PLCTAG_ERR_READ;
    } 

    /* we now have a valid reference to the request. */

    /* request can be used by more than one thread at once. */
    spin_block(&request->lock) {
        if(!request->resp_received) {
            rc = PLCTAG_STATUS_PENDING;
            break;
        }

        /* check to see if it was an abort on the session side. */
        if(request->status != PLCTAG_STATUS_OK) {
            rc = request->status;
            request->abort_request = 1;

            pdebug(DEBUG_WARN,"Session reported failure of request: %s.", plc_tag_decode_error(rc));

            tag->read_in_progress = 0;
            tag->offset = 0;

            /* TODO - why is this here? */
            tag->size = tag->elem_count * tag->elem_size;

            break;
        }
    }

    if(rc != PLCTAG_STATUS_OK) {
        if(rc_is_error(rc)) {
            /* the request is dead, from session side. */
            tag->read_in_progress = 0;
            tag->offset = 0;

            tag->req = NULL; 
        }

        pdebug(DEBUG_DETAIL, "Read not ready with status %s.", plc_tag_decode_error(rc));

        return rc;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}




/**
 * @brief Check the status of the write request
 * 
 * This function checks the request itself and updates the
 * tag if there are any failures or changes that need to be
 * made due to the request status.
 * 
 * The tag and the request must not be deleted out from underneath
 * this function.   Ideally both are held with write mutexes.
 * 
 * @return status of the request.
 * 
 */


int check_write_request_status(ab_tag_p tag, ab_request_p request)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!request) {
        tag->write_in_progress = 0;
        tag->offset = 0;

        pdebug(DEBUG_WARN,"Write in progress, but no request in flight!");

        return PLCTAG_ERR_WRITE;
    } 

    /* we now have a valid reference to the request. */

    /* request can be used by more than one thread at once. */
    spin_block(&request->lock) {
        if(!request->resp_received) {
            rc = PLCTAG_STATUS_PENDING;
            break;
        }

        /* check to see if it was an abort on the session side. */
        if(request->status != PLCTAG_STATUS_OK) {
            rc = request->status;
            request->abort_request = 1;

            pdebug(DEBUG_WARN,"Session reported failure of request: %s.", plc_tag_decode_error(rc));

            tag->write_in_progress = 0;
            tag->offset = 0;

            break;
        }
    }

    if(rc != PLCTAG_STATUS_OK) {
        if(rc_is_error(rc)) {
            /* the request is dead, from session side. */
            tag->read_in_progress = 0;
            tag->offset = 0;

            tag->req = NULL; 
        }

        pdebug(DEBUG_DETAIL, "Write not ready with status %s.", plc_tag_decode_error(rc));

        return rc;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}

#endif // __PROTOCOLS_AB_AB_COMMON_C__


#ifndef __PROTOCOLS_AB_CIP_C__
#define __PROTOCOLS_AB_CIP_C__

#include <ctype.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

static int skip_whitespace(const char *name, int *name_index);
static int parse_bit_segment(ab_tag_p tag, const char *name, int *name_index);
static int parse_symbolic_segment(ab_tag_p tag, const char *name, int *encoded_index, int *name_index);
static int parse_numeric_segment(ab_tag_p tag, const char *name, int *encoded_index, int *name_index);

static int match_numeric_segment(const char *path, size_t *path_index, uint8_t *conn_path, size_t *conn_path_index);
static int match_ip_addr_segment(const char *path, size_t *path_index, uint8_t *conn_path, size_t *conn_path_index);
static int match_dhp_addr_segment(const char *path, size_t *path_index, uint8_t *port, uint8_t *src_node, uint8_t *dest_node);

#define MAX_IP_ADDR_SEG_LEN (16)



int cip_encode_path(const char *path, int *needs_connection, plc_type_t plc_type, uint8_t **conn_path, uint8_t *conn_path_size, int *is_dhp, uint16_t *dhp_dest)
{
    size_t path_len = 0;
    size_t conn_path_index = 0;
    size_t path_index = 0;
    uint8_t dhp_port = 0;
    uint8_t dhp_src_node = 0;
    uint8_t dhp_dest_node = 0;
    uint8_t tmp_conn_path[MAX_CONN_PATH + MAX_IP_ADDR_SEG_LEN];

    pdebug(DEBUG_DETAIL, "Starting");

    *is_dhp = 0;

    path_len = (size_t)(ssize_t)str_length(path);

    while(path_index < path_len && path[path_index] && conn_path_index < MAX_CONN_PATH) {
        /* skip spaces before each segment */
        while(path[path_index] == ' ') {
            path_index++;
        }

        if(path[path_index] == ',') {
            /* skip separators. */
            pdebug(DEBUG_DETAIL, "Skipping separator character '%c'.", (char)path[path_index]);

            path_index++;
        } else if(match_numeric_segment(path, &path_index, tmp_conn_path, &conn_path_index) == PLCTAG_STATUS_OK) {
            pdebug(DEBUG_DETAIL, "Found numeric segment.");
        } else if(match_ip_addr_segment(path, &path_index, tmp_conn_path, &conn_path_index) == PLCTAG_STATUS_OK) {
            pdebug(DEBUG_DETAIL, "Found IP address segment.");
        } else if(match_dhp_addr_segment(path, &path_index, &dhp_port, &dhp_src_node, &dhp_dest_node) == PLCTAG_STATUS_OK) {
            pdebug(DEBUG_DETAIL, "Found DH+ address segment.");

            /* check if it is last. */
            if(path_index < path_len) {
                pdebug(DEBUG_WARN, "DH+ address must be the last segment in a path! %d %d", (int)(ssize_t)path_index, (int)(ssize_t)path_len);
                return PLCTAG_ERR_BAD_PARAM;
            }

            *is_dhp = 1;
        } else {
            /* unknown, cannot parse this! */
            pdebug(DEBUG_WARN, "Unable to parse remaining path string from position %d, \"%s\".", (int)(ssize_t)path_index, (char*)&path[path_index]);
            return PLCTAG_ERR_BAD_PARAM;
        }
    }

    if(conn_path_index >= MAX_CONN_PATH) {
        pdebug(DEBUG_WARN, "Encoded connection path is too long (%d >= %d).", (int)(ssize_t)conn_path_index, MAX_CONN_PATH);
        return PLCTAG_ERR_TOO_LARGE;
    }

    if(*is_dhp && (plc_type == AB_PLC_PLC5 || plc_type == AB_PLC_SLC || plc_type == AB_PLC_MLGX)) {
        /* DH+ bridging always needs a connection. */
        *needs_connection = 1;

        /* add the special PCCC/DH+ routing on the end. */
        tmp_conn_path[conn_path_index + 0] = 0x20;
        tmp_conn_path[conn_path_index + 1] = 0xA6;
        tmp_conn_path[conn_path_index + 2] = 0x24;
        tmp_conn_path[conn_path_index + 3] = dhp_port;
        tmp_conn_path[conn_path_index + 4] = 0x2C;
        tmp_conn_path[conn_path_index + 5] = 0x01;
        conn_path_index += 6;

        *dhp_dest = (uint16_t)dhp_dest_node;
    } else if(!*is_dhp) {
        if(*needs_connection) {
            pdebug(DEBUG_DETAIL, "PLC needs connection, adding path to the router object.");

            /*
             * we do a generic path to the router
             * object in the PLC.  But only if the PLC is
             * one that needs a connection.  For instance a
             * Micro850 needs to work in connected mode.
             */
            tmp_conn_path[conn_path_index + 0] = 0x20;
            tmp_conn_path[conn_path_index + 1] = 0x02;
            tmp_conn_path[conn_path_index + 2] = 0x24;
            tmp_conn_path[conn_path_index + 3] = 0x01;
            conn_path_index += 4;
        }

        *dhp_dest = 0;
    } else {
        /*
         *we had the special DH+ format and it was
         * either not last or not a PLC5/SLC.  That
         * is an error.
         */

        *dhp_dest = 0;

        return PLCTAG_ERR_BAD_PARAM;
    }

    /*
     * zero pad the path to a multiple of 16-bit
     * words.
     */
    pdebug(DEBUG_DETAIL,"IOI size before %d", conn_path_index);
    if(conn_path_index & 0x01) {
        tmp_conn_path[conn_path_index] = 0;
        conn_path_index++;
    }

    if(conn_path_index > 0) {
        /* allocate space for the connection path */
        *conn_path = (uint8_t*)mem_alloc((int)(unsigned int)conn_path_index);
        if(! *conn_path) {
            pdebug(DEBUG_WARN, "Unable to allocate connection path!");
            return PLCTAG_ERR_NO_MEM;
        }

        mem_copy(*conn_path, &tmp_conn_path[0], (int)(unsigned int)conn_path_index);
    } else {
        *conn_path = NULL;
    }

    *conn_path_size = (uint8_t)conn_path_index;

    pdebug(DEBUG_DETAIL, "Done");

    return PLCTAG_STATUS_OK;
}


int match_numeric_segment(const char *path, size_t *path_index, uint8_t *conn_path, size_t *conn_path_index)
{
    int val = 0;
    size_t p_index = *path_index;
    size_t c_index = *conn_path_index;

    pdebug(DEBUG_DETAIL, "Starting at position %d in string %s.", (int)(ssize_t)*path_index, path);

    while(isdigit(path[p_index])) {
        val = (val * 10) + (path[p_index] - '0');
        p_index++;
    }

    /* did we match anything? */
    if(p_index == *path_index) {
        pdebug(DEBUG_DETAIL,"Did not find numeric path segment at position %d.", (int)(ssize_t)p_index);
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* was the numeric segment valid? */
    if(val < 0 || val > 0x0F) {
        pdebug(DEBUG_WARN, "Numeric segment in path at position %d is out of bounds!", (int)(ssize_t)(*path_index));
        return PLCTAG_ERR_OUT_OF_BOUNDS;
    }

    /* store the encoded segment data. */
    conn_path[c_index] = (uint8_t)(unsigned int)(val);
    c_index++;
    *conn_path_index = c_index;

    /* skip trailing spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    pdebug(DEBUG_DETAIL, "Remaining path \"%s\".", &path[p_index]);

    /* bump past our last read character. */
    *path_index = p_index;

    pdebug(DEBUG_DETAIL, "Done. Found numeric segment %d.", val);

    return PLCTAG_STATUS_OK;
}


/*
 * match symbolic IP address segments.
 *  18,10.206.10.14 - port 2/A -> 10.206.10.14
 *  19,10.206.10.14 - port 3/B -> 10.206.10.14
 */

int match_ip_addr_segment(const char *path, size_t *path_index, uint8_t *conn_path, size_t *conn_path_index)
{
    uint8_t *addr_seg_len = NULL;
    int val = 0;
    size_t p_index = *path_index;
    size_t c_index = *conn_path_index;

    pdebug(DEBUG_DETAIL, "Starting at position %d in string %s.", (int)(ssize_t)*path_index, path);

    /* first part, the extended address marker*/
    val = 0;
    while(isdigit(path[p_index])) {
        val = (val * 10) + (path[p_index] - '0');
        p_index++;
    }

    if(val != 18 && val != 19) {
        pdebug(DEBUG_DETAIL, "Path segment at %d does not match IP address segment.", (int)(ssize_t)*path_index);
        return PLCTAG_ERR_NOT_FOUND;
    }

    if(val == 18) {
        pdebug(DEBUG_DETAIL, "Extended address on port A.");
    } else {
        pdebug(DEBUG_DETAIL, "Extended address on port B.");
    }

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    /* is the next character a comma? */
    if(path[p_index] != ',') {
        pdebug(DEBUG_DETAIL, "Not an IP address segment starting at position %d of path.  Remaining: \"%s\".",(int)(ssize_t)p_index, &path[p_index]);
        return PLCTAG_ERR_NOT_FOUND;
    }

    p_index++;

    /* start building up the connection path. */
    conn_path[c_index] = (uint8_t)(unsigned int)val;
    c_index++;

    /* point into the encoded path for the symbolic segment length. */
    addr_seg_len = &conn_path[c_index];
    *addr_seg_len = 0;
    c_index++;

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    /* get the first IP address digit. */
    val = 0;
    while(isdigit(path[p_index]) && (int)(unsigned int)(*addr_seg_len) < (MAX_IP_ADDR_SEG_LEN - 1)) {
        val = (val * 10) + (path[p_index] - '0');
        conn_path[c_index] = (uint8_t)path[p_index];
        c_index++;
        p_index++;
        (*addr_seg_len)++;
    }

    if(val < 0 || val > 255) {
        pdebug(DEBUG_WARN, "First IP address part is out of bounds (0 <= %d < 256) for an IPv4 octet.", val);
        return PLCTAG_ERR_BAD_PARAM;
    }

    pdebug(DEBUG_DETAIL, "First IP segment: %d.", val);

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    /* is the next character a dot? */
    if(path[p_index] != '.') {
        pdebug(DEBUG_DETAIL, "Unexpected character '%c' found at position %d in first IP address part.", path[p_index], p_index);
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* copy the dot. */
    conn_path[c_index] = (uint8_t)path[p_index];
    c_index++;
    p_index++;
    (*addr_seg_len)++;

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    /* get the second part. */
    val = 0;
    while(isdigit(path[p_index]) && (int)(unsigned int)(*addr_seg_len) < (MAX_IP_ADDR_SEG_LEN - 1)) {
        val = (val * 10) + (path[p_index] - '0');
        conn_path[c_index] = (uint8_t)path[p_index];
        c_index++;
        p_index++;
        (*addr_seg_len)++;
    }

    if(val < 0 || val > 255) {
        pdebug(DEBUG_WARN, "Second IP address part is out of bounds (0 <= %d < 256) for an IPv4 octet.", val);
        return PLCTAG_ERR_BAD_PARAM;
    }

    pdebug(DEBUG_DETAIL, "Second IP segment: %d.", val);

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    /* is the next character a dot? */
    if(path[p_index] != '.') {
        pdebug(DEBUG_DETAIL, "Unexpected character '%c' found at position %d in second IP address part.", path[p_index], p_index);
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* copy the dot. */
    conn_path[c_index] = (uint8_t)path[p_index];
    c_index++;
    p_index++;
    (*addr_seg_len)++;

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    /* get the third part. */
    val = 0;
    while(isdigit(path[p_index]) && (int)(unsigned int)(*addr_seg_len) < (MAX_IP_ADDR_SEG_LEN - 1)) {
        val = (val * 10) + (path[p_index] - '0');
        conn_path[c_index] = (uint8_t)path[p_index];
        c_index++;
        p_index++;
        (*addr_seg_len)++;
    }

    if(val < 0 || val > 255) {
        pdebug(DEBUG_WARN, "Third IP address part is out of bounds (0 <= %d < 256) for an IPv4 octet.", val);
        return PLCTAG_ERR_BAD_PARAM;
    }

    pdebug(DEBUG_DETAIL, "Third IP segment: %d.", val);

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    /* is the next character a dot? */
    if(path[p_index] != '.') {
        pdebug(DEBUG_DETAIL, "Unexpected character '%c' found at position %d in third IP address part.", path[p_index], p_index);
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* copy the dot. */
    conn_path[c_index] = (uint8_t)path[p_index];
    c_index++;
    p_index++;
    (*addr_seg_len)++;

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    /* get the fourth part. */
    val = 0;
    while(isdigit(path[p_index]) && (int)(unsigned int)(*addr_seg_len) < (MAX_IP_ADDR_SEG_LEN - 1)) {
        val = (val * 10) + (path[p_index] - '0');
        conn_path[c_index] = (uint8_t)path[p_index];
        c_index++;
        p_index++;
        (*addr_seg_len)++;
    }

    if(val < 0 || val > 255) {
        pdebug(DEBUG_WARN, "Fourth IP address part is out of bounds (0 <= %d < 256) for an IPv4 octet.", val);
        return PLCTAG_ERR_BAD_PARAM;
    }

    pdebug(DEBUG_DETAIL, "Fourth IP segment: %d.", val);

    /* We need to zero pad if the length is not a multiple of two. */
    if((*addr_seg_len) & (uint8_t)0x01) {
        conn_path[c_index] = (uint8_t)0;
        c_index++;
    }

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    /* set the return values. */
    *path_index = p_index;
    *conn_path_index = c_index;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}


/*
 * match DH+ address segments.
 *  A:1:2 - port 2/A -> DH+ node 2
 *  B:1:2 - port 3/B -> DH+ node 2
 *
 * A and B can be lowercase or numeric.
 */

int match_dhp_addr_segment(const char *path, size_t *path_index, uint8_t *port, uint8_t *src_node, uint8_t *dest_node)
{
    int val = 0;
    size_t p_index = *path_index;

    pdebug(DEBUG_DETAIL, "Starting at position %d in string %s.", (int)(ssize_t)*path_index, path);

    /* Get the port part. */
    switch(path[p_index]) {
        case 'A':
            /* fall through */
        case 'a':
            /* fall through */
        case '2':
            *port = 1;
            break;

        case 'B':
            /* fall through */
        case 'b':
            /* fall through */
        case '3':
            *port = 2;
            break;

        default:
            pdebug(DEBUG_DETAIL, "Character '%c' at position %d does not match start of DH+ segment.", path[p_index], (int)(ssize_t)p_index);
            return PLCTAG_ERR_NOT_FOUND;
            break;
    }

    p_index++;

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    /* is the next character a colon? */
    if(path[p_index] != ':') {
        pdebug(DEBUG_DETAIL, "Character '%c' at position %d does not match first colon expected in DH+ segment.", path[p_index], (int)(ssize_t)p_index);
        return PLCTAG_ERR_BAD_PARAM;
    }

    p_index++;

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    /* get the source node */
    val = 0;
    while(isdigit(path[p_index])) {
        val = (val * 10) + (path[p_index] - '0');
        p_index++;
    }

    /* is the source node a valid number? */
    if(val < 0 || val > 255) {
        pdebug(DEBUG_WARN, "Source node DH+ address part is out of bounds (0 <= %d < 256).", val);
        return PLCTAG_ERR_BAD_PARAM;
    }

    *src_node = (uint8_t)(unsigned int)val;

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    /* is the next character a colon? */
    if(path[p_index] != ':') {
        pdebug(DEBUG_DETAIL, "Character '%c' at position %d does not match the second colon expected in DH+ segment.", path[p_index], (int)(ssize_t)p_index);
        return PLCTAG_ERR_BAD_PARAM;
    }

    p_index++;

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    /* get the destination node */
    val = 0;
    while(isdigit(path[p_index])) {
        val = (val * 10) + (path[p_index] - '0');
        p_index++;
    }

    /* is the destination node a valid number? */
    if(val < 0 || val > 255) {
        pdebug(DEBUG_WARN, "Destination node DH+ address part is out of bounds (0 <= %d < 256).", val);
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* skip spaces */
    while(path[p_index] == ' ') {
        p_index++;
    }

    *dest_node = (uint8_t)(unsigned int)val;
    *path_index = p_index;

    pdebug(DEBUG_DETAIL, "Found DH+ path port:%d, source node:%d, destination node:%d.", (int)(unsigned int)*port, (int)(unsigned int)*src_node, (int)(unsigned int)*dest_node);

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}


/*
 * The EBNF is:
 *
 * tag ::= SYMBOLIC_SEG ( tag_seg )* ( bit_seg )?
 *
 * tag_seg ::= '.' SYMBOLIC_SEG
 *             '[' array_seg ']'
 *
 * bit_seg ::= '.' [0-9]+
 *
 * array_seg ::= NUMERIC_SEG ( ',' NUMERIC_SEG )*
 *
 * SYMBOLIC_SEG ::= [a-zA-Z]([a-zA-Z0-9_]*)
 *
 * NUMERIC_SEG ::= [0-9]+
 *
 */


int cip_encode_tag_name(ab_tag_p tag, const char *name)
{
    int rc = PLCTAG_STATUS_OK;
    int encoded_index = 0;
    int name_index = 0;
    int name_len = str_length(name);

    /* zero out the CIP encoded name size. Byte zero in the encoded name. */
    tag->encoded_name[encoded_index] = 0;
    encoded_index++;

    /* names must start with a symbolic segment. */
    if(parse_symbolic_segment(tag, name, &encoded_index, &name_index) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to parse initial symbolic segment in tag name %s!", name);
        return PLCTAG_ERR_BAD_PARAM;
    }

    while(name_index < name_len && encoded_index < MAX_TAG_NAME) {
        /* try to parse the different parts of the name. */
        if(name[name_index] == '.') {
            name_index++;
            /* could be a name segment or could be a bit identifier. */
            if(parse_symbolic_segment(tag, name, &encoded_index, &name_index) != PLCTAG_STATUS_OK) {
                /* try a bit identifier. */
                if(parse_bit_segment(tag, name, &name_index) == PLCTAG_STATUS_OK) {
                    pdebug(DEBUG_DETAIL, "Found bit identifier %u.", tag->bit);
                    break;
                } else {
                    pdebug(DEBUG_WARN, "Expected a symbolic segment or a bit identifier at position %d in tag name %s", name_index, name);
                    return PLCTAG_ERR_BAD_PARAM;
                }
            } else {
                pdebug(DEBUG_DETAIL, "Found symbolic segment ending at %d", name_index);
            }
        } else if (name[name_index] == '[') {
            int num_dimensions = 0;
            /* must be an array so look for comma separated numeric segments. */
            do {
                name_index++;
                num_dimensions++;

                skip_whitespace(name, &name_index);
                rc = parse_numeric_segment(tag, name, &encoded_index, &name_index);
                skip_whitespace(name, &name_index);
            } while(rc == PLCTAG_STATUS_OK && name[name_index] == ',' && num_dimensions < 3);

            /* must terminate with a closing ']' */
            if(name[name_index] != ']') {
                pdebug(DEBUG_WARN, "Bad tag name format, expected closing array bracket at %d in tag name %s!", name_index, name);
                return PLCTAG_ERR_BAD_PARAM;
            }

            /* step past the closing bracket. */
            name_index++;
        } else {
            pdebug(DEBUG_WARN,"Unexpected character at position %d in name string %s!", name_index, name);
            break;
        }
    }

    if(name_index != name_len) {
        pdebug(DEBUG_WARN, "Bad tag name format.  Tag must end with a bit identifier if one is present.");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* set the word count. */
    tag->encoded_name[0] = (uint8_t)((encoded_index -1)/2);
    tag->encoded_name_size = encoded_index;

    return PLCTAG_STATUS_OK;
}

int skip_whitespace(const char *name, int *name_index)
{
    while(name[*name_index] == ' ') {
        (*name_index)++;
    }

    return PLCTAG_STATUS_OK;
}


/*
 * A bit segment is simply an integer from 0 to 63 (inclusive). */
int parse_bit_segment(ab_tag_p tag, const char *name, int *name_index)
{
    const char *p, *q;
    long val;

    pdebug(DEBUG_DETAIL, "Starting with name index=%d.", *name_index);

    p = &name[*name_index];
    q = p;

    val = strtol((char *)p, (char **)&q, 10);

    /* sanity checks. */
    if(p == q) {
        /* no number. */
        pdebug(DEBUG_WARN,"Expected bit identifier or symbolic segment at position %d in tag name %s!", *name_index, name);
        return PLCTAG_ERR_BAD_PARAM;
    }

    if((val < 0) || (val >= 65536)) {
        pdebug(DEBUG_WARN,"Bit identifier must be between 0 and 255, inclusive, was %d!", (int)val);
        return PLCTAG_ERR_BAD_PARAM;
    }

    if(tag->elem_count != 1) {
        pdebug(DEBUG_WARN, "Bit tags must have only one element!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* bump name_index. */
    *name_index += (int)(q-p);
    tag->is_bit = 1;
    tag->bit = (int)val;

    return PLCTAG_STATUS_OK;
}


int parse_symbolic_segment(ab_tag_p tag, const char *name, int *encoded_index, int *name_index)
{
    int encoded_i = *encoded_index;
    int name_i = *name_index;
    int name_start = name_i;
    int seg_len_index = 0;
    int seg_len = 0;

    pdebug(DEBUG_DETAIL, "Starting with name index=%d and encoded name index=%d.", name_i, encoded_i);

    /* a symbolic segment must start with an alphabetic character or @, then can have digits or underscores. */
    if(!isalpha(name[name_i]) && name[name_i] != ':' && name[name_i] != '_' && name[name_i] != '@') {
        pdebug(DEBUG_DETAIL, "tag name at position %d is not the start of a symbolic segment.", name_i);
        return PLCTAG_ERR_NO_MATCH;
    }

    /* start building the encoded symbolic segment. */
    tag->encoded_name[encoded_i] = 0x91; /* start of symbolic segment. */
    encoded_i++;
    seg_len_index = encoded_i;
    tag->encoded_name[seg_len_index]++;
    encoded_i++;

    /* store the first character of the name. */
    tag->encoded_name[encoded_i] = (uint8_t)name[name_i];
    encoded_i++;
    name_i++;

    /* get the rest of the name. */
    while((isalnum(name[name_i]) || name[name_i] == ':' || name[name_i] == '_') && (encoded_i < (MAX_TAG_NAME - 1))) {
        tag->encoded_name[encoded_i] = (uint8_t)name[name_i];
        encoded_i++;
        tag->encoded_name[seg_len_index]++;
        name_i++;
    }

    seg_len = tag->encoded_name[seg_len_index];

    /* finish up the encoded name.   Space for the name must be a multiple of two bytes long. */
    if((tag->encoded_name[seg_len_index] & 0x01) && (encoded_i < MAX_TAG_NAME)) {
        tag->encoded_name[encoded_i] = 0;
        encoded_i++;
    }

    *encoded_index = encoded_i;
    *name_index = name_i;

    pdebug(DEBUG_DETAIL, "Parsed symbolic segment \"%.*s\" in tag name.", seg_len, &name[name_start]);

    return PLCTAG_STATUS_OK;
}


int parse_numeric_segment(ab_tag_p tag, const char *name, int *encoded_index, int *name_index)
{
    const char *p, *q;
    long val;

    pdebug(DEBUG_DETAIL, "Starting with name index=%d and encoded name index=%d.", *name_index, *encoded_index);

    p = &name[*name_index];
    q = p;

    val = strtol((char *)p, (char **)&q, 10);

    /* sanity checks. */
    if(p == q) {
        /* no number. */
        pdebug(DEBUG_WARN,"Expected numeric segment at position %d in tag name %s!", *name_index, name);
        return PLCTAG_ERR_BAD_PARAM;
    }

    if(val < 0) {
        pdebug(DEBUG_WARN,"Numeric segment must be greater than or equal to zero, was %d!", (int)val);
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* bump name_index. */
    *name_index += (int)(q-p);

    /* encode the segment. */
    if(val > 0xFFFF) {
        tag->encoded_name[*encoded_index] = (uint8_t)0x2A; /* 4-byte segment value. */
        (*encoded_index)++;

        tag->encoded_name[*encoded_index] = (uint8_t)0; /* padding. */
        (*encoded_index)++;

        tag->encoded_name[*encoded_index] = (uint8_t)val & 0xFF;
        (*encoded_index)++;
        tag->encoded_name[*encoded_index] = (uint8_t)((val >> 8) & 0xFF);
        (*encoded_index)++;
        tag->encoded_name[*encoded_index] = (uint8_t)((val >> 16) & 0xFF);
        (*encoded_index)++;
        tag->encoded_name[*encoded_index] = (uint8_t)((val >> 24) & 0xFF);
        (*encoded_index)++;

        pdebug(DEBUG_DETAIL, "Parsed 4-byte numeric segment of value %u.", (uint32_t)val);
    } else if(val > 0xFF) {
        tag->encoded_name[*encoded_index] = (uint8_t)0x29; /* 2-byte segment value. */
        (*encoded_index)++;

        tag->encoded_name[*encoded_index] = (uint8_t)0; /* padding. */
        (*encoded_index)++;

        tag->encoded_name[*encoded_index] = (uint8_t)val & 0xFF;
        (*encoded_index)++;
        tag->encoded_name[*encoded_index] = (uint8_t)((val >> 8) & 0xFF);
        (*encoded_index)++;

        pdebug(DEBUG_DETAIL, "Parsed 2-byte numeric segment of value %u.", (uint32_t)val);
    } else {
        tag->encoded_name[*encoded_index] = (uint8_t)0x28; /* 1-byte segment value. */
        (*encoded_index)++;

        tag->encoded_name[*encoded_index] = (uint8_t)val & 0xFF;
        (*encoded_index)++;

        pdebug(DEBUG_DETAIL, "Parsed 1-byte numeric segment of value %u.", (uint32_t)val);
    }

    pdebug(DEBUG_DETAIL, "Done with name index=%d and encoded name index=%d.", *name_index, *encoded_index);

    return PLCTAG_STATUS_OK;
}





//
//typedef enum { START, ARRAY, DOT, NAME } encode_state_t;
//
///*
// * cip_encode_tag_name()
// *
// * This takes a LGX-style tag name like foo[14].blah and
// * turns it into an IOI path/string.
// */
//
//int cip_encode_tag_name_old(ab_tag_p tag,const char *name)
//{
//    uint8_t *data = tag->encoded_name;
//    const char *p = name;
//    uint8_t *word_count = NULL;
//    uint8_t *dp = NULL;
//    uint8_t *name_len;
//    encode_state_t state;
//    int first_num = 1;
//
//    /* reserve room for word count for IOI string. */
//    word_count = data;
//    dp = data + 1;
//
//    state = START;
//
//    while(*p && (dp - data) < MAX_TAG_NAME) {
//        switch(state) {
//        case START:
//
//            /* must start with an alpha character or _ or :. */
//            if(isalpha(*p) || *p == '_' || *p == ':') {
//                state = NAME;
//            } else if(*p == '.') {
//                state = DOT;
//            } else if(*p == '[') {
//                state = ARRAY;
//            } else {
//                return 0;
//            }
//
//            break;
//
//        case NAME:
//            *dp = 0x91; /* start of ASCII name */
//            dp++;
//            name_len = dp;
//            *name_len = 0;
//            dp++;
//
//            while(isalnum(*p) || *p == '_' || *p == ':') {
//                *dp = (uint8_t)*p;
//                dp++;
//                p++;
//                (*name_len)++;
//            }
//
//            /* must pad the name to a multiple of two bytes */
//            if(*name_len & 0x01) {
//                *dp = 0;
//                dp++;
//            }
//
//            state = START;
//
//            break;
//
//        case ARRAY:
//            /* move the pointer past the [ character */
//            p++;
//
//            do {
//                long int val;
//                char *np = NULL;
//
//                /* skip past a commas. */
//                if(!first_num && *p == ',') {
//                    p++;
//                }
//
//                /* get the numeric value. */
//                val = strtol(p, &np, 10);
//
//                if(np == p) {
//                    /* we must have a number */
//                    pdebug(DEBUG_WARN, "Expected number in tag name string!");
//                    return 0;
//                } else {
//                    pdebug(DEBUG_DETAIL, "got number %ld.", val);
//                }
//
//                if(val < 0) {
//                    pdebug(DEBUG_WARN, "Array index must be greater than or equal to zero!");
//                    return 0;
//                }
//
//                first_num = 0;
//
//                p = np;
//
//                if(val > 0xFFFF) {
//                    *dp = 0x2A;
//                    dp++;  /* 4-byte value */
//                    *dp = 0;
//                    dp++;     /* padding */
//
//                    /* copy the value in little-endian order */
//                    *dp = (uint8_t)val & 0xFF;
//                    dp++;
//                    *dp = (uint8_t)((val >> 8) & 0xFF);
//                    dp++;
//                    *dp = (uint8_t)((val >> 16) & 0xFF);
//                    dp++;
//                    *dp = (uint8_t)((val >> 24) & 0xFF);
//                    dp++;
//                } else if(val > 0xFF) {
//                    *dp = 0x29;
//                    dp++;  /* 2-byte value */
//                    *dp = 0;
//                    dp++;     /* padding */
//
//                    /* copy the value in little-endian order */
//                    *dp = (uint8_t)val & 0xFF;
//                    dp++;
//                    *dp = (uint8_t)((val >> 8) & 0xFF);
//                    dp++;
//                } else {
//                    *dp = 0x28;
//                    dp++;  /* 1-byte value */
//                    *dp = (uint8_t)val;
//                    dp++;     /* value */
//                }
//
//                /* eat spaces */
//                while(*p == ' ') {
//                    p++;
//                }
//            } while(*p == ',');
//
//            if(*p != ']') {
//                return 0;
//            }
//
//            p++;
//
//            state = START;
//
//            break;
//
//        case DOT:
//            p++;
//            state = START;
//            break;
//
//        default:
//            /* this should never happen */
//            return 0;
//
//            break;
//        }
//    }
//
//    if((dp - data) >= MAX_TAG_NAME) {
//        pdebug(DEBUG_WARN,"Encoded tag name is too long!  Length=%d", (dp - data));
//        return 0;
//    }
//
//    /* word_count is in units of 16-bit integers, do not
//     * count the word_count value itself.
//     */
//    *word_count = (uint8_t)((dp - data)-1)/2;
//
//    /* store the size of the whole result */
//    tag->encoded_name_size = (int)(dp - data);
//
//    return 1;
//}


#endif // __PROTOCOLS_AB_CIP_C__


#ifndef __PROTOCOLS_AB_EIP_CIP_SPECIAL_C__
#define __PROTOCOLS_AB_EIP_CIP_SPECIAL_C__

#include <ctype.h>

/* tag listing packet format is as follows for controller tags:

CIP Tag Info command
    uint8_t request_service    0x55
    uint8_t request_path_size  3 - 6 bytes
    uint8_t   0x20    get class
    uint8_t   0x6B    tag info/symbol class
    uint8_t   0x25    get instance (16-bit)
    uint8_t   0x00    padding
    uint8_t   0x00    instance byte 0
    uint8_t   0x00    instance byte 1
    uint16_t  0x04    number of attributes to get
    uint16_t  0x02    attribute #2 - symbol type
    uint16_t  0x07    attribute #7 - base type size (array element) in bytes
    uint16_t  0x08    attribute #8 - array dimensions (3xu32)
    uint16_t  0x01    attribute #1 - symbol name

*/

/* tag listing packet format is as follows for program tags:

CIP Tag Info command
    uint8_t request_service    0x55
    uint8_t request_path_size  N bytes
      uint8_t   0x91    Symbolic segment header
      uint8_t   name_length   Length in bytes.
      uint8_t   name[N] program name, i.e. 'PROGRAM:foobar'
      (uint8_t padding) optional if program name is odd length.
      uint8_t   0x20    get class
      uint8_t   0x6B    tag info/symbol class
      uint8_t   0x25    get instance (16-bit)
      uint8_t   0x00    padding
      uint8_t   0x00    instance byte 0
      uint8_t   0x00    instance byte 1
    uint16_t  0x04    number of attributes to get
    uint16_t  0x02    attribute #2 - symbol type
    uint16_t  0x07    attribute #7 - base type size (array element) in bytes
    uint16_t  0x08    attribute #8 - array dimensions (3xu32)
    uint16_t  0x01    attribute #1 - symbol name

*/

//
//START_PACK typedef struct {
//    uint8_t request_service;    /* AB_EIP_CMD_CIP_LIST_TAGS=0x55 */
//    uint8_t request_path_size;  /* 3 word = 6 bytes */
//    uint8_t request_path[4];    /* MAGIC
//                                    0x20    get class
//                                    0x6B    tag info/symbol class
//                                    0x25    get instance (16-bit)
//                                    0x00    padding
//                                    0x00    instance byte 0
//                                    0x00    instance byte 1
//                                */
//    uint16_le instance_id;      /* actually last two bytes above */
//    uint16_le num_attributes;   /* 0x04    number of attributes to get */
//    uint16_le requested_attributes[4];  /*
//                                            0x02 attribute #2 - symbol type
//                                            0x07 attribute #7 - base type size (array element) in bytes
//                                            0x08    attribute #8 - array dimensions (3xu32)
//                                            0x01    attribute #1 - symbol name
//                                        */
//
//} END_PACK tag_list_req_DEAD;

/*
 * This is a pseudo UDT structure for each tag entry when listing all the tags
 * in a PLC.
 */

START_PACK typedef struct {
        uint32_le instance_id;  /* monotonically increasing but not contiguous */
        uint16_le symbol_type;   /* type of the symbol. */
        uint16_le element_length; /* length of one array element in bytes. */
        uint32_le array_dims[3];  /* array dimensions. */
        uint16_le string_len;   /* string length count. */
        //uint8_t string_name[82]; /* MAGIC string name bytes (string_len of them, zero padded) */
} END_PACK tag_list_entry;


/* raw tag functions */
//static int raw_tag_read_start(ab_tag_p tag);
static int raw_tag_tickler(ab_tag_p tag);
static int raw_tag_write_start(ab_tag_p tag);
static int raw_tag_check_write_status_connected(ab_tag_p tag);
static int raw_tag_check_write_status_unconnected(ab_tag_p tag);
static int raw_tag_build_write_request_connected(ab_tag_p tag);
static int raw_tag_build_write_request_unconnected(ab_tag_p tag);



/* listing tag functions. */
static int listing_tag_read_start(ab_tag_p tag);
static int listing_tag_tickler(ab_tag_p tag);
//static int listing_tag_write_start(ab_tag_p tag);
static int listing_tag_check_read_status_connected(ab_tag_p tag);
static int listing_tag_build_read_request_connected(ab_tag_p tag);



/* UDT tag functions. */
static int udt_tag_read_start(ab_tag_p tag);
static int udt_tag_tickler(ab_tag_p tag);
//static int listing_tag_write_start(ab_tag_p tag);
static int udt_tag_check_read_metadata_status_connected(ab_tag_p tag);
static int udt_tag_build_read_metadata_request_connected(ab_tag_p tag);
static int udt_tag_check_read_fields_status_connected(ab_tag_p tag);
static int udt_tag_build_read_fields_request_connected(ab_tag_p tag);


/* define the vtable for raw tag type. */
struct tag_vtable_t raw_tag_vtable = {
    (tag_vtable_func)ab_tag_abort, /* shared */
    (tag_vtable_func)NULL, /* read */
    (tag_vtable_func)ab_tag_status, /* shared */
    (tag_vtable_func)raw_tag_tickler,
    (tag_vtable_func)raw_tag_write_start,
    (tag_vtable_func)NULL, /* wake_plc */

    /* attribute accessors */
    ab_get_int_attrib,
    ab_set_int_attrib
};

/* define the vtable for listing tag type. */
struct tag_vtable_t listing_tag_vtable = {
    (tag_vtable_func)ab_tag_abort, /* shared */
    (tag_vtable_func)listing_tag_read_start,
    (tag_vtable_func)ab_tag_status, /* shared */
    (tag_vtable_func)listing_tag_tickler,
    (tag_vtable_func)NULL, /* write */
    (tag_vtable_func)NULL, /* wake_plc */

    /* attribute accessors */
    ab_get_int_attrib,
    ab_set_int_attrib
};


/* define the vtable for udt tag type. */
struct tag_vtable_t udt_tag_vtable = {
    (tag_vtable_func)ab_tag_abort, /* shared */
    (tag_vtable_func)udt_tag_read_start,
    (tag_vtable_func)ab_tag_status, /* shared */
    (tag_vtable_func)udt_tag_tickler,
    (tag_vtable_func)NULL, /* write */
    (tag_vtable_func)NULL, /* wake_plc */

    /* attribute accessors */
    ab_get_int_attrib,
    ab_set_int_attrib
};



//tag_byte_order_t listing_tag_logix_byte_order = {
//    .is_allocated = 0,
//
//    .str_is_defined = 1,
//    .str_is_counted = 1,
//    .str_is_fixed_length = 0,
//    .str_is_zero_terminated = 0,
//    .str_is_byte_swapped = 0,
//
//    .str_count_word_bytes = 2,
//    .str_max_capacity = 0,
//    .str_total_length = 0,
//    .str_pad_bytes = 0,
//
//    .int16_order = {0,1},
//    .int32_order = {0,1,2,3},
//    .int64_order = {0,1,2,3,4,5,6,7},
//    .float32_order = {0,1,2,3},
//    .float64_order = {0,1,2,3,4,5,6,7},
//};

/* strings are zero terminated. */
tag_byte_order_t udt_tag_logix_byte_order = {
    .is_allocated = 0,

    .str_is_defined = 1,
    .str_is_counted = 0,
    .str_is_fixed_length = 0,
    .str_is_zero_terminated = 1,
    .str_is_byte_swapped = 0,

    .str_count_word_bytes = 0,
    .str_max_capacity = 0,
    .str_total_length = 0,
    .str_pad_bytes = 0,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};



/*************************************************************************
 **************************** API Functions ******************************
 ************************************************************************/


/* Raw tag functions */


int setup_raw_tag(ab_tag_p tag)
{
    pdebug(DEBUG_DETAIL, "Starting.");

    /* set up raw tag. */
    tag->special_tag = 1;
    tag->elem_type = AB_TYPE_TAG_RAW;
    tag->elem_count = 1;
    tag->elem_size = 1;

    tag->byte_order = &logix_tag_byte_order;

    pdebug(DEBUG_DETAIL, "Setting vtable to %p.", &raw_tag_vtable);

    tag->vtable = &raw_tag_vtable;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;

}




int raw_tag_tickler(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW,"Starting.");

    if(tag->read_in_progress) {
        pdebug(DEBUG_WARN, "Something started a read on a raw tag.  This is not supported!");
        tag->read_in_progress = 0;
        tag->read_in_flight = 0;

        return rc;
    }

    if(tag->write_in_progress) {
        if(tag->use_connected_msg) {
            rc = raw_tag_check_write_status_connected(tag);
        } else {
            rc = raw_tag_check_write_status_unconnected(tag);
        }

        tag->status = (int8_t)rc;

        /* if the operation completed, make a note so that the callback will be called. */
        if(!tag->write_in_progress) {
            pdebug(DEBUG_DETAIL, "Write complete.");
            tag->write_complete = 1;
        }

        pdebug(DEBUG_SPEW, "Done.");

        return rc;
    }

    pdebug(DEBUG_SPEW, "Done.  No operation in progress.");

    return tag->status;
}


/*
 * raw_tag_write_start
 *
 * This must be called from one thread alone, or while the tag mutex is
 * locked.
 *
 * The routine starts the process of writing to a tag.
 */

int raw_tag_write_start(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting");

    if(tag->read_in_progress) {
        pdebug(DEBUG_WARN, "Raw tag found with a read in flight!");
        return PLCTAG_ERR_BAD_STATUS;
    }

    if(tag->write_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    /* the write is now in flight */
    tag->write_in_progress = 1;

    if(tag->use_connected_msg) {
        rc = raw_tag_build_write_request_connected(tag);
    } else {
        rc = raw_tag_build_write_request_unconnected(tag);
    }

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to build write request!");
        tag->write_in_progress = 0;

        return rc;
    }

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}





/*
 * raw_tag_check_write_status_connected
 *
 * This routine must be called with the tag mutex locked.  It checks the current
 * status of a write operation.  If the write is done, it triggers the clean up.
 */

static int raw_tag_check_write_status_connected(ab_tag_p tag)
{
    eip_cip_co_resp* cip_resp;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_write_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Write request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    /* point to the data */
    cip_resp = (eip_cip_co_resp*)(request->data);

    do {
        if (le2h16(cip_resp->encap_command) != AB_EIP_CONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", cip_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (le2h32(cip_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(cip_resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        /* the client needs to handle the raw CIP response. */

        // if (cip_resp->reply_service != (AB_EIP_CMD_CIP_WRITE_FRAG | AB_EIP_CMD_CIP_OK)
        //     && cip_resp->reply_service != (AB_EIP_CMD_CIP_WRITE | AB_EIP_CMD_CIP_OK)
        //     && cip_resp->reply_service != (AB_EIP_CMD_CIP_RMW | AB_EIP_CMD_CIP_OK)) {
        //     pdebug(DEBUG_WARN, "CIP response reply service unexpected: %d", cip_resp->reply_service);
        //     rc = PLCTAG_ERR_BAD_DATA;
        //     break;
        // }

        // if (cip_resp->status != AB_CIP_STATUS_OK && cip_resp->status != AB_CIP_STATUS_FRAG) {
        //     pdebug(DEBUG_WARN, "CIP read failed with status: 0x%x %s", cip_resp->status, decode_cip_error_short((uint8_t *)&cip_resp->status));
        //     pdebug(DEBUG_INFO, decode_cip_error_long((uint8_t *)&cip_resp->status));
        //     rc = decode_cip_error_code((uint8_t *)&cip_resp->status);
        //     break;
        // }
    } while(0);

    /* write is done in one way or another. */
    tag->write_in_progress = 0;

    if(rc == PLCTAG_STATUS_OK) {
        /* copy the data into the tag. */
        uint8_t *data_start = (uint8_t *)(&cip_resp->reply_service);
        uint8_t *data_end = request->data + (request->request_size);
        int data_size = (int)(unsigned int)(data_end - data_start);
        uint8_t *tag_data_buffer = (uint8_t*)mem_realloc(tag->data, data_size);

        if(tag_data_buffer) {
            tag->data = tag_data_buffer;
            tag->size = data_size;

            mem_copy(tag->data, data_start, data_size);
        } else {
            pdebug(DEBUG_WARN, "Unable to reallocate tag data buffer!");
            rc = PLCTAG_ERR_NO_MEM;
        }
    } else {
        pdebug(DEBUG_WARN,"Write failed!");

        tag->offset = 0;
    }

    /* clean up the request. */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}





/*
 * raw_tag_check_write_status_unconnected
 *
 * This routine must be called with the tag mutex locked.  It checks the current
 * status of a write operation.  If the write is done, it triggers the clean up.
 */

static int raw_tag_check_write_status_unconnected(ab_tag_p tag)
{
    eip_cip_uc_resp* cip_resp;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_write_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Write request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    /* point to the data */
    cip_resp = (eip_cip_uc_resp*)(request->data);

    do {
        if (le2h16(cip_resp->encap_command) != AB_EIP_CONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", cip_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (le2h32(cip_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(cip_resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        /* the client needs to handle the raw CIP response. */

        // if (cip_resp->reply_service != (AB_EIP_CMD_CIP_WRITE_FRAG | AB_EIP_CMD_CIP_OK)
        //     && cip_resp->reply_service != (AB_EIP_CMD_CIP_WRITE | AB_EIP_CMD_CIP_OK)
        //     && cip_resp->reply_service != (AB_EIP_CMD_CIP_RMW | AB_EIP_CMD_CIP_OK)) {
        //     pdebug(DEBUG_WARN, "CIP response reply service unexpected: %d", cip_resp->reply_service);
        //     rc = PLCTAG_ERR_BAD_DATA;
        //     break;
        // }

        // if (cip_resp->status != AB_CIP_STATUS_OK && cip_resp->status != AB_CIP_STATUS_FRAG) {
        //     pdebug(DEBUG_WARN, "CIP read failed with status: 0x%x %s", cip_resp->status, decode_cip_error_short((uint8_t *)&cip_resp->status));
        //     pdebug(DEBUG_INFO, decode_cip_error_long((uint8_t *)&cip_resp->status));
        //     rc = decode_cip_error_code((uint8_t *)&cip_resp->status);
        //     break;
        // }
    } while(0);

    /* write is done in one way or another. */
    tag->write_in_progress = 0;

    if(rc == PLCTAG_STATUS_OK) {
        /* copy the data into the tag. */
        uint8_t *data_start = (uint8_t *)(&cip_resp->reply_service);
        uint8_t *data_end = data_start + le2h16(cip_resp->cpf_udi_item_length);
        int data_size = (int)(unsigned int)(data_end - data_start);
        uint8_t *tag_data_buffer = (uint8_t*)mem_realloc(tag->data, data_size);

        if(tag_data_buffer) {
            tag->data = tag_data_buffer;
            tag->size = data_size;

            mem_copy(tag->data, data_start, data_size);
        } else {
            pdebug(DEBUG_WARN, "Unable to reallocate tag data buffer!");
            rc = PLCTAG_ERR_NO_MEM;
        }
    } else {
        pdebug(DEBUG_WARN,"Write failed!");

        tag->offset = 0;
    }

    /* clean up the request. */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}






int raw_tag_build_write_request_connected(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_co_req* cip = NULL;
    uint8_t* data = NULL;
    ab_request_p req = NULL;

    pdebug(DEBUG_INFO, "Starting.");

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    if(tag->size > session_get_max_payload(tag->session)) {
        pdebug(DEBUG_WARN, "Amount to write exceeds negotiated session size %d!", session_get_max_payload(tag->session));
        return PLCTAG_ERR_TOO_LARGE;
    }

    cip = (eip_cip_co_req*)(req->data);

    /* point to the end of the struct */
    data = (req->data) + sizeof(eip_cip_co_req);

    /*
     * set up the embedded CIP request packet.  The user/client needs
     * to set up the entire CIP request.   We just copy it here.
     */

    /* copy the tag data into the request */
    mem_copy(data, tag->data, tag->size);
    data += tag->size;

    /* now we go back and fill in the fields of the static part */

    /* encap fields */
    cip->encap_command = h2le16(AB_EIP_CONNECTED_SEND); /* ALWAYS 0x0070 Unconnected Send*/

    /* router timeout */
    cip->router_timeout = h2le16(1); /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    cip->cpf_item_count = h2le16(2);                 /* ALWAYS 2 */
    cip->cpf_cai_item_type = h2le16(AB_EIP_ITEM_CAI);/* ALWAYS 0x00A1 connected address item */
    cip->cpf_cai_item_length = h2le16(4);            /* ALWAYS 4, size of connection ID*/
    cip->cpf_cdi_item_type = h2le16(AB_EIP_ITEM_CDI);/* ALWAYS 0x00B1 - connected Data Item */
    cip->cpf_cdi_item_length = h2le16((uint16_t)(data - (uint8_t*)(&cip->cpf_conn_seq_num))); /* REQ: fill in with length of remaining data. */

    /* set the size of the request */
    req->request_size = (int)(data - (req->data));

    /* allow packing if the tag allows it. */
    req->allow_packing = tag->allow_packing;

    /* reset the tag size so that incoming data overwrites the old. */
    tag->size = 0;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}




int raw_tag_build_write_request_unconnected(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_uc_req* cip = NULL;
    uint8_t* data = NULL;
    uint8_t *embed_start = NULL;
    uint8_t *embed_end = NULL;
    ab_request_p req = NULL;

    pdebug(DEBUG_INFO, "Starting.");

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    cip = (eip_cip_uc_req*)(req->data);

    /* point to the end of the struct */
    data = (req->data) + sizeof(eip_cip_uc_req);

    embed_start = data;

    /*
     * set up the embedded CIP read packet
     * The format is:
     *
     * uint8_t cmd
     * LLA formatted name
     * data type to write
     * uint16_t # of elements to write
     * data to write
     */

    /*
     * set up the embedded CIP request packet.  The user/client needs
     * to set up the entire CIP request.   We just copy it here.
     */

    /* copy the tag data into the request */
    mem_copy(data, tag->data, tag->size);
    data += tag->size;

    /* now we go back and fill in the fields of the static part */

    /* mark the end of the embedded packet */
    embed_end = data;

    /*
     * after the embedded packet, we need to tell the message router
     * how to get to the target device.
     */

    /* Now copy in the routing information for the embedded message */
    *data = (tag->session->conn_path_size) / 2; /* in 16-bit words */
    data++;
    *data = 0;
    data++;    /* copy the tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* encap fields */
    cip->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND); /* ALWAYS 0x006F Unconnected Send*/

    /* router timeout */
    cip->router_timeout = h2le16(1); /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    cip->cpf_item_count = h2le16(2);                  /* ALWAYS 2 */
    cip->cpf_nai_item_type = h2le16(AB_EIP_ITEM_NAI); /* ALWAYS 0 */
    cip->cpf_nai_item_length = h2le16(0);             /* ALWAYS 0 */
    cip->cpf_udi_item_type = h2le16(AB_EIP_ITEM_UDI); /* ALWAYS 0x00B2 - Unconnected Data Item */
    cip->cpf_udi_item_length = h2le16((uint16_t)(data - (uint8_t*)(&(cip->cm_service_code)))); /* REQ: fill in with length of remaining data. */

    /* CM Service Request - Connection Manager */
    cip->cm_service_code = AB_EIP_CMD_UNCONNECTED_SEND; /* 0x52 Unconnected Send */
    cip->cm_req_path_size = 2;                          /* 2, size in 16-bit words of path, next field */
    cip->cm_req_path[0] = 0x20;                         /* class */
    cip->cm_req_path[1] = 0x06;                         /* Connection Manager */
    cip->cm_req_path[2] = 0x24;                         /* instance */
    cip->cm_req_path[3] = 0x01;                         /* instance 1 */

    /* Unconnected send needs timeout information */
    cip->secs_per_tick = AB_EIP_SECS_PER_TICK; /* seconds per tick */
    cip->timeout_ticks = AB_EIP_TIMEOUT_TICKS; /* timeout = srd_secs_per_tick * src_timeout_ticks */

    /* size of embedded packet */
    cip->uc_cmd_length = h2le16((uint16_t)(embed_end - embed_start));

    /* set the size of the request */
    req->request_size = (int)(data - (req->data));

    /* allow packing if the tag allows it. */
    req->allow_packing = tag->allow_packing;

    /* reset the tag size so that incoming data overwrites the old. */
    tag->size = 0;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}






/******************************************************************
 ******************* tag listing functions ************************
 ******************************************************************/




/*
 * Handle tag listing tag set up.
 *
 * There are two main cases here: 1) a bare tag listing, 2) a program tag listing.
 * We know that we got here because the string "@tags" was in the name.
 */

int setup_tag_listing_tag(ab_tag_p tag, const char *name)
{
    int rc = PLCTAG_STATUS_OK;
    char **tag_parts = NULL;

    pdebug(DEBUG_DETAIL, "Starting.");

    do {
        /* is it a bare tag listing? */
        if(str_cmp_i(name, "@tags") == 0) {
            pdebug(DEBUG_DETAIL, "Tag is a bare tag listing tag.");
            break;
        }

        /* is it a program tag listing request? */
        if(str_length(name) >= str_length("PROGRAM:x.@tags")) {
            tag_parts = str_split(name, ".");

            /* check to make sure that we have at least one part. */
            if(!tag_parts) {
                pdebug(DEBUG_WARN, "Tag %s is not a tag listing request.", name);
                rc = PLCTAG_ERR_BAD_PARAM;
                break;
            }

            /* check that we have exactly two parts. */
            if(tag_parts[0] != NULL && tag_parts[1] != NULL && tag_parts[2] == NULL) {
                /* we have exactly two parts. Make sure the last part is "@tags" */
                if(str_cmp_i(tag_parts[1], "@tags") != 0) {
                    pdebug(DEBUG_WARN, "Tag %s is not a tag listing request.", name);
                    rc = PLCTAG_ERR_BAD_PARAM;
                    break;
                }

                if(str_length(tag_parts[0]) <= str_length("PROGRAM:x")) {
                    pdebug(DEBUG_WARN, "Tag %s is not a tag listing request.", name);
                    rc = PLCTAG_ERR_BAD_PARAM;
                    break;
                }

                /* make sure the first part is "PROGRAM:" */
                if(str_cmp_i_n(tag_parts[0], "PROGRAM:", str_length("PROGRAM:"))) {
                    pdebug(DEBUG_WARN, "Tag %s is not a tag listing request.", name);
                    rc = PLCTAG_ERR_NOT_FOUND;
                    break;
                }

                /* we have a program tag request! */
                if(cip_encode_tag_name(tag, tag_parts[0]) != PLCTAG_STATUS_OK) {
                    pdebug(DEBUG_WARN, "Tag %s program listing is not able to be encoded!", name);
                    rc = PLCTAG_ERR_BAD_PARAM;
                    break;
                }
            } else {
                pdebug(DEBUG_WARN, "Tag %s is not a tag listing request.", name);
                rc = PLCTAG_ERR_NOT_FOUND;
                break;
            }
        } else {
            pdebug(DEBUG_WARN, "Program tag %s listing tag string malformed.");
            rc = PLCTAG_ERR_BAD_PARAM;
            break;
        }
    } while(0);

    /* clean up */
    if(tag_parts) {
        mem_free(tag_parts);
    }

    /* did we find a listing tag? */
    if(rc == PLCTAG_STATUS_OK) {
        /* yes we did */
        tag->special_tag = 1;
        tag->elem_type = AB_TYPE_TAG_ENTRY;
        tag->elem_count = 1;
        tag->elem_size = 1;

        tag->byte_order = &listing_tag_logix_byte_order;

        tag->vtable = &listing_tag_vtable;

        pdebug(DEBUG_INFO, "Done. Found tag listing tag name %s.", name);
    } else {
        pdebug(DEBUG_WARN, "Done. Tag %s is not a well-formed tag listing name, error %s.", name, plc_tag_decode_error(rc));
    }

    return rc;
}



/*
 * listing_tag_read_start
 *
 * This function must be called only from within one thread, or while
 * the tag's mutex is locked.
 *
 * The function starts the process of getting tag data from the PLC.
 */

int listing_tag_read_start(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting");

    if(tag->write_in_progress) {
        pdebug(DEBUG_WARN, "A write is in progress on a listing tag!");
        return PLCTAG_ERR_BAD_STATUS;
    }

    if(tag->read_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    /* mark the tag read in progress */
    tag->read_in_progress = 1;

    /* build the new request */
    rc = listing_tag_build_read_request_connected(tag);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to build read request!");

        tag->read_in_progress = 0;

        return rc;
    }

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}



int listing_tag_tickler(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW,"Starting.");

    if (tag->read_in_progress) {
        if(tag->elem_type == AB_TYPE_TAG_RAW) {
            pdebug(DEBUG_WARN, "Something started a read on a raw tag.  This is not supported!");
            tag->read_in_progress = 0;
            tag->read_in_flight = 0;
        }

        rc = listing_tag_check_read_status_connected(tag);
        // if (rc != PLCTAG_STATUS_PENDING) {
        //     pdebug(DEBUG_WARN,"Error %s getting tag list read status!", plc_tag_decode_error(rc));
        // }

        tag->status = (int8_t)rc;

        /* if the operation completed, make a note so that the callback will be called. */
        if(!tag->read_in_progress) {
            pdebug(DEBUG_DETAIL, "Read complete.");
            tag->read_complete = 1;
        }

        pdebug(DEBUG_SPEW,"Done.  Read in progress.");

        return rc;
    }

    pdebug(DEBUG_SPEW, "Done.  No operation in progress.");

    return tag->status;
}









/*
 * listing_tag_check_read_status_connected
 *
 * This routine checks for any outstanding tag list requests.  It will
 * terminate when there is no data in the response and the error is not "more data".
 *
 * This is not thread-safe!  It should be called with the tag mutex
 * locked!
 */

static int listing_tag_check_read_status_connected(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_co_resp* cip_resp;
    uint8_t* data;
    uint8_t* data_end;
    int partial_data = 0;
    ab_request_p request = NULL;

    static int symbol_index=0;


    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_read_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Read request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    /* point to the data */
    cip_resp = (eip_cip_co_resp*)(request->data);

    /* point to the start of the data */
    data = (request->data) + sizeof(eip_cip_co_resp);

    /* point the end of the data */
    data_end = (request->data + le2h16(cip_resp->encap_length) + sizeof(eip_encap));

    /* check the status */
    do {
        ptrdiff_t payload_size = (data_end - data);

        if (le2h16(cip_resp->encap_command) != AB_EIP_CONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", cip_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (le2h32(cip_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(cip_resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if (cip_resp->reply_service != (AB_EIP_CMD_CIP_LIST_TAGS | AB_EIP_CMD_CIP_OK) ) {
            pdebug(DEBUG_WARN, "CIP response reply service unexpected: %d", cip_resp->reply_service);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (cip_resp->status != AB_CIP_STATUS_OK && cip_resp->status != AB_CIP_STATUS_FRAG) {
            pdebug(DEBUG_WARN, "CIP read failed with status: 0x%x %s", cip_resp->status, decode_cip_error_short((uint8_t *)&cip_resp->status));
            pdebug(DEBUG_INFO, decode_cip_error_long((uint8_t *)&cip_resp->status));
            rc = decode_cip_error_code((uint8_t *)&cip_resp->status);
            break;
        }

        /* check to see if this is a partial response. */
        partial_data = (cip_resp->status == AB_CIP_STATUS_FRAG);

        /*
         * check to see if there is any data to process.  If this is a packed
         * response, there might not be.
         */
        if(payload_size > 0) {
            uint8_t *current_entry_data = data;
            int new_size = (int)payload_size + tag->offset;

            /* copy the data into the tag and realloc if we need more space. */

            if(new_size > tag->size) {
                uint8_t *new_buffer = NULL;

                tag->elem_count = tag->size = new_size;

                pdebug(DEBUG_DETAIL, "Increasing tag buffer size to %d bytes.", new_size);

                new_buffer = (uint8_t*)mem_realloc(tag->data, new_size);
                if(!new_buffer) {
                    pdebug(DEBUG_WARN, "Unable to reallocate tag data memory!");
                    rc = PLCTAG_ERR_NO_MEM;
                    break;
                }

                tag->data = new_buffer;
                tag->elem_count = tag->size = new_size;
            }

            /* copy the data into the tag's data buffer. */
            mem_copy(tag->data + tag->offset, data, (int)payload_size);

            tag->offset += (int)payload_size;

            pdebug(DEBUG_DETAIL, "current offset %d", tag->offset);

            /* scan through the data to get the next ID to use. */
            while((data_end - current_entry_data) > 0) {
                tag_list_entry *current_entry = (tag_list_entry*)current_entry_data;

                /* first element is the symbol instance ID */
                tag->next_id = (uint16_t)(le2h32(current_entry->instance_id) + 1);

                pdebug(DEBUG_DETAIL, "Next ID: %d", tag->next_id);

                /* skip past to the next instance. */
                current_entry_data += (sizeof(*current_entry) + le2h16(current_entry->string_len));

                symbol_index++;
            }
        } else {
            pdebug(DEBUG_DETAIL, "Response returned no data and no error.");
        }

        /* set the return code */
        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    /* are we actually done? */
    if (rc == PLCTAG_STATUS_OK) {
        /* keep going if we are not done yet. */
        if (partial_data) {
            /* call read start again to get the next piece */
            pdebug(DEBUG_DETAIL, "calling listing_tag_build_read_request_connected() to get the next chunk.");
            rc = listing_tag_build_read_request_connected(tag);
        } else {
            /* done! */
            pdebug(DEBUG_DETAIL, "Done reading tag list data!");

            pdebug(DEBUG_DETAIL, "total symbols: %d", symbol_index);

            tag->elem_count = tag->offset;

            tag->first_read = 0;
            tag->offset = 0;
            tag->next_id = 0;

            /* this read is done. */
            tag->read_in_progress = 0;
        }
    }

    /* this is not an else clause because the above if could result in bad rc. */
    if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
        /* error ! */
        pdebug(DEBUG_WARN, "Error received: %s!", plc_tag_decode_error(rc));

        tag->offset = 0;
        tag->next_id = 0;

        /* clean up everything. */
        ab_tag_abort(tag);
    }

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}




int listing_tag_build_read_request_connected(ab_tag_p tag)
{
    eip_cip_co_req* cip = NULL;
    //tag_list_req *list_req = NULL;
    ab_request_p req = NULL;
    int rc = PLCTAG_STATUS_OK;
    uint8_t *data_start = NULL;
    uint8_t *data = NULL;
    uint16_le tmp_u16 = UINT16_LE_INIT(0);

    pdebug(DEBUG_INFO, "Starting.");

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    /* point the request struct at the buffer */
    cip = (eip_cip_co_req*)(req->data);

    /* point to the end of the struct */
    data_start = data = (uint8_t*)(cip + 1);

    /*
     * set up the embedded CIP tag list request packet
        uint8_t request_service;    AB_EIP_CMD_CIP_LIST_TAGS=0x55
        uint8_t request_path_size;  3 word = 6 bytes
        uint8_t request_path[6];        0x20    get class
                                        0x6B    tag info/symbol class
                                        0x25    get instance (16-bit)
                                        0x00    padding
                                        0x00    instance byte 0
                                        0x00    instance byte 1
        uint16_le instance_id;      NOTE! this is the last two bytes above for convenience!
        uint16_le num_attributes;   0x04    number of attributes to get
        uint16_le requested_attributes[4];      0x02 attribute #2 - symbol type
                                                0x07 attribute #7 - base type size (array element) in bytes
                                                0x08    attribute #8 - array dimensions (3xu32)
                                                0x01    attribute #1 - symbol name
    */

    *data = AB_EIP_CMD_CIP_LIST_TAGS;
    data++;

    /* request path size, in 16-bit words */
    *data = (uint8_t)(3 + ((tag->encoded_name_size-1)/2)); /* size in words of routing header + routing and instance ID. */
    data++;

    /* add in the encoded name, but without the leading word count byte! */
    if(tag->encoded_name_size > 1) {
        mem_copy(data, &tag->encoded_name[1], (tag->encoded_name_size-1));
        data += (tag->encoded_name_size-1);
    }

    /* add in the routing header . */

    /* first the fixed part. */
    data[0] = 0x20; /* class type */
    data[1] = 0x6B; /* tag info/symbol class */
    data[2] = 0x25; /* 16-bit instance ID type */
    data[3] = 0x00; /* padding */
    data += 4;

    /* now the instance ID */
    tmp_u16 = h2le16((uint16_t)tag->next_id);
    mem_copy(data, &tmp_u16, (int)sizeof(tmp_u16));
    data += (int)sizeof(tmp_u16);

    /* set up the request itself.  We are asking for a number of attributes. */

    /* set up the request attributes, first the number of attributes. */
    tmp_u16 = h2le16((uint16_t)4);  /* MAGIC, we have four attributes we want. */
    mem_copy(data, &tmp_u16, (int)sizeof(tmp_u16));
    data += (int)sizeof(tmp_u16);

    /* first attribute: symbol type */
    tmp_u16 = h2le16((uint16_t)0x02);  /* MAGIC, symbol type. */
    mem_copy(data, &tmp_u16, (int)sizeof(tmp_u16));
    data += (int)sizeof(tmp_u16);

    /* second attribute: base type size in bytes */
    tmp_u16 = h2le16((uint16_t)0x07);  /* MAGIC, element size in bytes. */
    mem_copy(data, &tmp_u16, (int)sizeof(tmp_u16));
    data += (int)sizeof(tmp_u16);

    /* third attribute: tag array dimensions */
    tmp_u16 = h2le16((uint16_t)0x08);  /* MAGIC, array dimensions. */
    mem_copy(data, &tmp_u16, (int)sizeof(tmp_u16));
    data += (int)sizeof(tmp_u16);

    /* fourth attribute: symbol/tag name */
    tmp_u16 = h2le16((uint16_t)0x01);  /* MAGIC, symbol name. */
    mem_copy(data, &tmp_u16, (int)sizeof(tmp_u16));
    data += (int)sizeof(tmp_u16);

    /* now we go back and fill in the fields of the static part */

    /* encap fields */
    cip->encap_command = h2le16(AB_EIP_CONNECTED_SEND); /* ALWAYS 0x0070 Connected Send*/

    /* router timeout */
    cip->router_timeout = h2le16(1); /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    cip->cpf_item_count = h2le16(2);                 /* ALWAYS 2 */
    cip->cpf_cai_item_type = h2le16(AB_EIP_ITEM_CAI);/* ALWAYS 0x00A1 connected address item */
    cip->cpf_cai_item_length = h2le16(4);            /* ALWAYS 4, size of connection ID*/
    cip->cpf_cdi_item_type = h2le16(AB_EIP_ITEM_CDI);/* ALWAYS 0x00B1 - connected Data Item */
    cip->cpf_cdi_item_length = h2le16((uint16_t)((int)(data - data_start) + (int)sizeof(cip->cpf_conn_seq_num)));

    /* set the size of the request */
    req->request_size = (int)((int)sizeof(*cip) + (int)(data - data_start));

    req->allow_packing = tag->allow_packing;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}









/******************************************************************
 ******************* UDT listing functions ************************
 ******************************************************************/




/*
 * Handle UDT tag set up.
 */

int setup_udt_tag(ab_tag_p tag, const char *name)
{
    int rc = PLCTAG_STATUS_OK;
    const char *tag_id_str = name + str_length("@udt/");
    int tag_id = 0;

    pdebug(DEBUG_DETAIL, "Starting.");

    /* decode the UDT ID */
    rc = str_to_int(tag_id_str, &tag_id);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Badly formatted or missing UDT id in UDT string %s!", name);
        return PLCTAG_ERR_BAD_PARAM;
    }

    if(tag_id < 0 || tag_id > 4095) {
        pdebug(DEBUG_WARN, "UDT ID must be between 0 and 4095 but was %d!", tag_id);
        return PLCTAG_ERR_OUT_OF_BOUNDS;
    }

    /*fill in the blanks. */
    tag->udt_id = (uint16_t)(unsigned int)tag_id;
    tag->special_tag = 1;
    tag->elem_type = AB_TYPE_TAG_UDT;
    tag->elem_count = 1;
    tag->elem_size = 1;

    tag->byte_order = &udt_tag_logix_byte_order;

    tag->vtable = &udt_tag_vtable;

    pdebug(DEBUG_INFO, "Done. Found UDT tag name %s.", name);

    return rc;
}



/*
 * udt_tag_read_start
 *
 * This function must be called only from within one thread, or while
 * the tag's mutex is locked.
 *
 * The function starts the process of getting UDT data from the PLC.
 */

int udt_tag_read_start(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting");

    if(tag->write_in_progress) {
        pdebug(DEBUG_WARN, "A write is in progress on a UDT tag!");
        return PLCTAG_ERR_BAD_STATUS;
    }

    if(tag->read_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    /* mark the tag read in progress */
    tag->read_in_progress = 1;

    /* set up the state for the requests (there are two!) */
    tag->udt_get_fields = 0;
    tag->offset = 0;

    /* build the new request */
    rc = udt_tag_build_read_metadata_request_connected(tag);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to build read request!");

        tag->read_in_progress = 0;

        return rc;
    }

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}



int udt_tag_tickler(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW,"Starting.");

    if (tag->read_in_progress) {
        if(tag->elem_type == AB_TYPE_TAG_RAW) {
            pdebug(DEBUG_WARN, "Something started a read on a raw tag.  This is not supported!");
            tag->read_in_progress = 0;
            tag->read_in_flight = 0;
        }

        if(tag->udt_get_fields) {
            rc = udt_tag_check_read_fields_status_connected(tag);
        } else {
            rc = udt_tag_check_read_metadata_status_connected(tag);
        }

        tag->status = (int8_t)rc;

        /* if the operation completed, make a note so that the callback will be called. */
        if(!tag->read_in_progress) {
            pdebug(DEBUG_DETAIL, "Read complete.");
            tag->read_complete = 1;
        }

        pdebug(DEBUG_SPEW,"Done.  Read in progress.");

        return rc;
    }

    pdebug(DEBUG_SPEW, "Done.  No operation in progress.");

    return tag->status;
}









/*
 * udt_tag_check_read_metadata_status_connected
 *
 * This routine checks for any outstanding tag udt requests.  It will
 * terminate when there is no data in the response and the error is not "more data".
 *
 * This is not thread-safe!  It should be called with the tag mutex
 * locked!
 */

static int udt_tag_check_read_metadata_status_connected(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_co_resp* cip_resp;
    uint8_t* data;
    uint8_t* data_end;
    int partial_data = 0;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_read_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Read request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    /* point to the data */
    cip_resp = (eip_cip_co_resp*)(request->data);

    /* point to the start of the data */
    data = (request->data) + sizeof(eip_cip_co_resp);

    /* point the end of the data */
    data_end = (request->data + le2h16(cip_resp->encap_length) + sizeof(eip_encap));

    /* check the status */
    do {
        ptrdiff_t payload_size = (data_end - data);

        if (le2h16(cip_resp->encap_command) != AB_EIP_CONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", cip_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (le2h32(cip_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(cip_resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if (cip_resp->reply_service != (AB_EIP_CMD_CIP_GET_ATTR_LIST | AB_EIP_CMD_CIP_OK) ) {
            pdebug(DEBUG_WARN, "CIP response reply service unexpected: %d", cip_resp->reply_service);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (cip_resp->status != AB_CIP_STATUS_OK && cip_resp->status != AB_CIP_STATUS_FRAG) {
            pdebug(DEBUG_WARN, "CIP read failed with status: 0x%x %s", cip_resp->status, decode_cip_error_short((uint8_t *)&cip_resp->status));
            pdebug(DEBUG_INFO, decode_cip_error_long((uint8_t *)&cip_resp->status));
            rc = decode_cip_error_code((uint8_t *)&cip_resp->status);
            break;
        }

        /* check to see if this is a partial response. */
        partial_data = (cip_resp->status == AB_CIP_STATUS_FRAG);

        /*
         * check to see if there is any data to process.  If this is a packed
         * response, there might not be.
         */
        if(payload_size > 0 && !partial_data) {
            uint8_t *new_buffer = NULL;
            int new_size = 14; /* MAGIC, size of the header below */
            uint32_le tmp_u32;
            uint16_le tmp_u16;
            uint8_t *payload = (uint8_t *)(cip_resp + 1);

            /*
             * We are going to build a 14-byte fake header in the buffer:
             *
             * Bytes   Meaning
             * 0-1     16-bit UDT ID
             * 2-5     32-bit UDT member description size, in 32-bit words.
             * 6-9     32-bit UDT instance size, in bytes.
             * 10-11   16-bit UDT number of members (fields).
             * 12-13   16-bit UDT handle/type.
             */

            pdebug(DEBUG_DETAIL, "Increasing tag buffer size to %d bytes.", new_size); /* MAGIC */

            new_buffer = (uint8_t*)mem_realloc(tag->data, new_size);
            if(!new_buffer) {
                pdebug(DEBUG_WARN, "Unable to reallocate tag data memory!");
                rc = PLCTAG_ERR_NO_MEM;
                break;
            }

            tag->data = new_buffer;
            tag->size = new_size;
            tag->elem_count = 1;
            tag->elem_size = new_size;

            /* fill in the data. */

            /* put in the UDT ID */
            tmp_u16 = h2le16(tag->udt_id);
            mem_copy(tag->data + 0, &tmp_u16, (int)(unsigned int)(sizeof(tmp_u16)));

            /* copy in the UDT member description size in 32-bit words */
            mem_copy(tag->data + 2, payload + 6, (int)(unsigned int)(sizeof(tmp_u32)));

            /* copy in the UDT instance size in bytes */
            mem_copy(tag->data + 6, payload + 14, (int)(unsigned int)(sizeof(tmp_u32)));

            /* copy in the UDT number of members */
            mem_copy(tag->data + 10, payload + 22, (int)(unsigned int)(sizeof(tmp_u16)));

            /* copy in the UDT number of members */
            mem_copy(tag->data + 12, payload + 28, (int)(unsigned int)(sizeof(tmp_u16)));

            pdebug(DEBUG_DETAIL, "current size %d", tag->size);
            pdebug_dump_bytes(DEBUG_DETAIL, tag->data, tag->size);
        } else {
            pdebug(DEBUG_DETAIL, "Response returned no data and no error.");
        }

        /* set the return code */
        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    /* are we actually done? */
    if (rc == PLCTAG_STATUS_OK) {
        /* keep going if we are not done yet. */
        if (partial_data) {
            /* call read start again to try again.  The data returned might be zero bytes if this is a packed result */
            pdebug(DEBUG_DETAIL, "calling udt_tag_build_read_metadata_request_connected() to try again.");
            rc = udt_tag_build_read_metadata_request_connected(tag);
        } else {
            /* done! */
            pdebug(DEBUG_DETAIL, "Done reading udt metadata!");

            tag->elem_count = 1;
            tag->offset = 0;
            tag->udt_get_fields = 1;

            pdebug(DEBUG_DETAIL, "calling udt_tag_build_read_fields_request_connected() to get field data.");
            rc = udt_tag_build_read_fields_request_connected(tag);
        }
    }

    /* this is not an else clause because the above if could result in bad rc. */
    if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
        /* error ! */
        pdebug(DEBUG_WARN, "Error received: %s!", plc_tag_decode_error(rc));

        tag->offset = 0;
        tag->udt_get_fields = 0;

        /* clean up everything. */
        ab_tag_abort(tag);
    }

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}




int udt_tag_build_read_metadata_request_connected(ab_tag_p tag)
{
    eip_cip_co_req* cip = NULL;
    //tag_list_req *list_req = NULL;
    ab_request_p req = NULL;
    int rc = PLCTAG_STATUS_OK;
    uint8_t *data_start = NULL;
    uint8_t *data = NULL;
    uint16_le tmp_u16 = UINT16_LE_INIT(0);

    pdebug(DEBUG_INFO, "Starting.");

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    /* point the request struct at the buffer */
    cip = (eip_cip_co_req*)(req->data);

    /* point to the end of the struct */
    data_start = data = (uint8_t*)(cip + 1);

    /*
     * set up the embedded CIP UDT metadata request packet
        uint8_t request_service;    AB_EIP_CMD_CIP_GET_ATTR_LIST=0x03
        uint8_t request_path_size;  3 word = 6 bytes
        uint8_t request_path[6];        0x20    get class
                                        0x6C    UDT class
                                        0x25    get instance (16-bit)
                                        0x00    padding
                                        0x00    instance byte 0
                                        0x00    instance byte 1
        uint16_le instance_id;      NOTE! this is the last two bytes above for convenience!
        uint16_le num_attributes;   0x04    number of attributes to get
        uint16_le requested_attributes[4];      0x04    attribute #4 - Number of 32-bit words in the template definition.
                                                0x05    attribute #5 - Number of bytes in the structure on the wire.
                                                0x02    attribute #2 - Number of structure members.
                                                0x01    attribute #1 - Handle/type of structure.
    */

    *data = AB_EIP_CMD_CIP_GET_ATTR_LIST;
    data++;

    /* request path size, in 16-bit words */
    *data = (uint8_t)(3); /* size in words of routing header + routing and instance ID. */
    data++;

    /* add in the routing header . */

    /* first the fixed part. */
    data[0] = 0x20; /* class type */
    data[1] = 0x6C; /* UDT class */
    data[2] = 0x25; /* 16-bit instance ID type */
    data[3] = 0x00; /* padding */
    data += 4;

    /* now the instance ID */
    tmp_u16 = h2le16((uint16_t)tag->udt_id);
    mem_copy(data, &tmp_u16, (int)sizeof(tmp_u16));
    data += (int)sizeof(tmp_u16);

    /* set up the request itself.  We are asking for a number of attributes. */

    /* set up the request attributes, first the number of attributes. */
    tmp_u16 = h2le16((uint16_t)4);  /* MAGIC, we have four attributes we want. */
    mem_copy(data, &tmp_u16, (int)sizeof(tmp_u16));
    data += (int)sizeof(tmp_u16);

    /* first attribute: symbol type */
    tmp_u16 = h2le16((uint16_t)0x04);  /* MAGIC, Total field definition size in 32-bit words. */
    mem_copy(data, &tmp_u16, (int)sizeof(tmp_u16));
    data += (int)sizeof(tmp_u16);

    /* second attribute: base type size in bytes */
    tmp_u16 = h2le16((uint16_t)0x05);  /* MAGIC, struct size in bytes. */
    mem_copy(data, &tmp_u16, (int)sizeof(tmp_u16));
    data += (int)sizeof(tmp_u16);

    /* third attribute: tag array dimensions */
    tmp_u16 = h2le16((uint16_t)0x02);  /* MAGIC, number of structure members. */
    mem_copy(data, &tmp_u16, (int)sizeof(tmp_u16));
    data += (int)sizeof(tmp_u16);

    /* fourth attribute: symbol/tag name */
    tmp_u16 = h2le16((uint16_t)0x01);  /* MAGIC, struct type/handle. */
    mem_copy(data, &tmp_u16, (int)sizeof(tmp_u16));
    data += (int)sizeof(tmp_u16);

    /* now we go back and fill in the fields of the static part */

    /* encap fields */
    cip->encap_command = h2le16(AB_EIP_CONNECTED_SEND); /* ALWAYS 0x0070 Connected Send*/

    /* router timeout */
    cip->router_timeout = h2le16(1); /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    cip->cpf_item_count = h2le16(2);                 /* ALWAYS 2 */
    cip->cpf_cai_item_type = h2le16(AB_EIP_ITEM_CAI);/* ALWAYS 0x00A1 connected address item */
    cip->cpf_cai_item_length = h2le16(4);            /* ALWAYS 4, size of connection ID*/
    cip->cpf_cdi_item_type = h2le16(AB_EIP_ITEM_CDI);/* ALWAYS 0x00B1 - connected Data Item */
    cip->cpf_cdi_item_length = h2le16((uint16_t)((int)(data - data_start) + (int)sizeof(cip->cpf_conn_seq_num)));

    /* set the size of the request */
    req->request_size = (int)((int)sizeof(*cip) + (int)(data - data_start));

    req->allow_packing = tag->allow_packing;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}








/*
 * udt_tag_check_read_fields_status_connected
 *
 * This routine checks for any outstanding tag udt field data requests.  It will
 * terminate when there is no data in the response and the error is not "more data".
 *
 * This is not thread-safe!  It should be called with the tag mutex
 * locked!
 */

int udt_tag_check_read_fields_status_connected(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_co_resp* cip_resp;
    uint8_t* data;
    uint8_t* data_end;
    int partial_data = 0;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_read_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Read request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    /* point to the data */
    cip_resp = (eip_cip_co_resp*)(request->data);

    /* point to the start of the data */
    data = (request->data) + sizeof(eip_cip_co_resp);

    /* point the end of the data */
    data_end = (request->data + le2h16(cip_resp->encap_length) + sizeof(eip_encap));

    /* check the status */
    do {
        ptrdiff_t payload_size = (data_end - data);

        if (le2h16(cip_resp->encap_command) != AB_EIP_CONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", cip_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (le2h32(cip_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(cip_resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if (cip_resp->reply_service != (AB_EIP_CMD_CIP_READ | AB_EIP_CMD_CIP_OK) ) {
            pdebug(DEBUG_WARN, "CIP response reply service unexpected: %d", cip_resp->reply_service);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (cip_resp->status != AB_CIP_STATUS_OK && cip_resp->status != AB_CIP_STATUS_FRAG) {
            pdebug(DEBUG_WARN, "CIP read failed with status: 0x%x %s", cip_resp->status, decode_cip_error_short((uint8_t *)&cip_resp->status));
            pdebug(DEBUG_INFO, decode_cip_error_long((uint8_t *)&cip_resp->status));
            rc = decode_cip_error_code((uint8_t *)&cip_resp->status);
            break;
        }

        /* check to see if this is a partial response. */
        partial_data = (cip_resp->status == AB_CIP_STATUS_FRAG);

        /*
         * check to see if there is any data to process.  If this is a packed
         * response, there might not be.
         */
        if(payload_size > 0) {
            uint8_t *new_buffer = NULL;
            int new_size = (int)(tag->size) + (int)payload_size;

            pdebug(DEBUG_DETAIL, "Increasing tag buffer size to %d bytes.", new_size);

            new_buffer = (uint8_t*)mem_realloc(tag->data, new_size);
            if(!new_buffer) {
                pdebug(DEBUG_WARN, "Unable to reallocate tag data memory!");
                rc = PLCTAG_ERR_NO_MEM;
                break;
            }

            /* copy the data into the tag's data buffer. */
            mem_copy(new_buffer + tag->offset + 14, data, (int)payload_size); /* MAGIC, offset plus the header. */

            tag->data = new_buffer;
            tag->size = new_size;
            tag->elem_size = new_size;

            tag->offset += (int)payload_size;

            pdebug(DEBUG_DETAIL, "payload of %d (%x) bytes resulting in current offset %d", (int)payload_size, (int)payload_size, tag->offset);
        } else {
            pdebug(DEBUG_DETAIL, "Response returned no data and no error.");
        }

        /* set the return code */
        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    /* are we actually done? */
    if (rc == PLCTAG_STATUS_OK) {
        /* keep going if we are not done yet. */
        if (partial_data) {
            /* call read start again to try again.  The data returned might be zero bytes if this is a packed result */
            pdebug(DEBUG_DETAIL, "calling udt_tag_build_read_metadata_request_connected() to try again.");
            rc = udt_tag_build_read_fields_request_connected(tag);
        } else {
            /* done! */
            pdebug(DEBUG_DETAIL, "Done reading udt field data.  Tag buffer contains:");
            pdebug_dump_bytes(DEBUG_DETAIL, tag->data, tag->size);

            tag->elem_count = 1;

            /* this read is done. */
            tag->udt_get_fields = 0;
            tag->read_in_progress = 0;
            tag->offset = 0;
        }
    }

    /* this is not an else clause because the above if could result in bad rc. */
    if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
        /* error ! */
        pdebug(DEBUG_WARN, "Error received: %s!", plc_tag_decode_error(rc));

        tag->offset = 0;
        tag->udt_get_fields = 0;

        /* clean up everything. */
        ab_tag_abort(tag);
    }

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}




int udt_tag_build_read_fields_request_connected(ab_tag_p tag)
{
    eip_cip_co_req* cip = NULL;
    //tag_list_req *list_req = NULL;
    ab_request_p req = NULL;
    int rc = PLCTAG_STATUS_OK;
    uint8_t *data_start = NULL;
    uint8_t *data = NULL;
    uint16_le tmp_u16 = UINT16_LE_INIT(0);
    uint32_le tmp_u32 = UINT32_LE_INIT(0);
    uint32_t total_size = 0;
    uint32_t neg_4 = (~(uint32_t)4) + 1; /* twos-complement */

    pdebug(DEBUG_INFO, "Starting.");

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    /* calculate the total size we need to get. */
    mem_copy(&tmp_u32, tag->data + 2, (int)(unsigned int)(sizeof(tmp_u32)));
    total_size = (4 * le2h32(tmp_u32)) - 23; /* formula according to the docs. */

    pdebug(DEBUG_DETAIL, "Calculating total size of request, %d to %d.", (int)(unsigned int)total_size, (int)(unsigned int)((total_size + (uint32_t)3) & (uint32_t)neg_4));

    /* make the total size a multiple of 4 bytes.  Round up. */
    total_size = (total_size + 3) & (uint32_t)neg_4;

    /* point the request struct at the buffer */
    cip = (eip_cip_co_req*)(req->data);

    /* point to the end of the struct */
    data_start = data = (uint8_t*)(cip + 1);

    /*
     * set up the embedded CIP UDT metadata request packet
        uint8_t request_service;        AB_EIP_CMD_CIP_READ=0x4C
        uint8_t request_path_size;      3 word = 6 bytes
        uint8_t request_path[6];        0x20    get class
                                        0x6C    UDT class
                                        0x25    get instance (16-bit)
                                        0x00    padding
                                        0x00    instance byte 0
                                        0x00    instance byte 1
        uint32_t offset;                Byte offset in ongoing requests.
        uint16_t total_size;            Total size of request in bytes.
    */

    *data = AB_EIP_CMD_CIP_READ;
    data++;

    /* request path size, in 16-bit words */
    *data = (uint8_t)(3); /* size in words of routing header + routing and instance ID. */
    data++;

    /* add in the routing header . */

    /* first the fixed part. */
    data[0] = 0x20; /* class type */
    data[1] = 0x6C; /* UDT class */
    data[2] = 0x25; /* 16-bit instance ID type */
    data[3] = 0x00; /* padding */
    data += 4;

    /* now the instance ID */
    tmp_u16 = h2le16((uint16_t)tag->udt_id);
    mem_copy(data, &tmp_u16, (int)sizeof(tmp_u16));
    data += (int)sizeof(tmp_u16);

    /* set the offset */
    tmp_u32 = h2le32((uint32_t)(tag->offset));
    mem_copy(data, &tmp_u32, (int)(unsigned int)sizeof(tmp_u32));
    data += sizeof(tmp_u32);

    /* set the total size */
    pdebug(DEBUG_DETAIL, "Total size %d less offset %d gives %d bytes for the request.", total_size, tag->offset, ((int)(unsigned int)total_size - tag->offset));
    tmp_u16 = h2le16((uint16_t)(total_size - (uint16_t)(unsigned int)tag->offset));
    mem_copy(data, &tmp_u16, (int)(unsigned int)sizeof(tmp_u16));
    data += sizeof(tmp_u16);

    /* now we go back and fill in the fields of the static part */

    /* encap fields */
    cip->encap_command = h2le16(AB_EIP_CONNECTED_SEND); /* ALWAYS 0x0070 Connected Send*/

    /* router timeout */
    cip->router_timeout = h2le16(1); /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    cip->cpf_item_count = h2le16(2);                 /* ALWAYS 2 */
    cip->cpf_cai_item_type = h2le16(AB_EIP_ITEM_CAI);/* ALWAYS 0x00A1 connected address item */
    cip->cpf_cai_item_length = h2le16(4);            /* ALWAYS 4, size of connection ID*/
    cip->cpf_cdi_item_type = h2le16(AB_EIP_ITEM_CDI);/* ALWAYS 0x00B1 - connected Data Item */
    cip->cpf_cdi_item_length = h2le16((uint16_t)((int)(data - data_start) + (int)sizeof(cip->cpf_conn_seq_num)));

    /* set the size of the request */
    req->request_size = (int)((int)sizeof(*cip) + (int)(data - data_start));

    req->allow_packing = tag->allow_packing;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}

#endif // __PROTOCOLS_AB_EIP_CIP_SPECIAL_C__


#ifndef __PROTOCOLS_AB_EIP_CIP_C__
#define __PROTOCOLS_AB_EIP_CIP_C__

#include <ctype.h>

/* tag listing packet format is as follows for controller tags:

CIP Tag Info command
    uint8_t request_service    0x55
    uint8_t request_path_size  3 - 6 bytes
    uint8_t   0x20    get class
    uint8_t   0x6B    tag info/symbol class
    uint8_t   0x25    get instance (16-bit)
    uint8_t   0x00    padding
    uint8_t   0x00    instance byte 0
    uint8_t   0x00    instance byte 1
    uint16_t  0x04    number of attributes to get
    uint16_t  0x02    attribute #2 - symbol type
    uint16_t  0x07    attribute #7 - base type size (array element) in bytes
    uint16_t  0x08    attribute #8 - array dimensions (3xu32)
    uint16_t  0x01    attribute #1 - symbol name

*/

/* tag listing packet format is as follows for program tags:

CIP Tag Info command
    uint8_t request_service    0x55
    uint8_t request_path_size  N bytes
      uint8_t   0x91    Symbolic segment header
      uint8_t   name_length   Length in bytes.
      uint8_t   name[N] program name, i.e. 'PROGRAM:foobar'
      (uint8_t padding) optional if program name is odd length.
      uint8_t   0x20    get class
      uint8_t   0x6B    tag info/symbol class
      uint8_t   0x25    get instance (16-bit)
      uint8_t   0x00    padding
      uint8_t   0x00    instance byte 0
      uint8_t   0x00    instance byte 1
    uint16_t  0x04    number of attributes to get
    uint16_t  0x02    attribute #2 - symbol type
    uint16_t  0x07    attribute #7 - base type size (array element) in bytes
    uint16_t  0x08    attribute #8 - array dimensions (3xu32)
    uint16_t  0x01    attribute #1 - symbol name

*/

//
//START_PACK typedef struct {
//    uint8_t request_service;    /* AB_EIP_CMD_CIP_LIST_TAGS=0x55 */
//    uint8_t request_path_size;  /* 3 word = 6 bytes */
//    uint8_t request_path[4];    /* MAGIC
//                                    0x20    get class
//                                    0x6B    tag info/symbol class
//                                    0x25    get instance (16-bit)
//                                    0x00    padding
//                                    0x00    instance byte 0
//                                    0x00    instance byte 1
//                                */
//    uint16_le instance_id;      /* actually last two bytes above */
//    uint16_le num_attributes;   /* 0x04    number of attributes to get */
//    uint16_le requested_attributes[4];  /*
//                                            0x02 attribute #2 - symbol type
//                                            0x07 attribute #7 - base type size (array element) in bytes
//                                            0x08    attribute #8 - array dimensions (3xu32)
//                                            0x01    attribute #1 - symbol name
//                                        */
//
//} END_PACK tag_list_req_DEAD;

/*
 * This is a pseudo UDT structure for each tag entry when listing all the tags
 * in a PLC.
 */
#if 0
START_PACK typedef struct {
        uint32_le instance_id;  /* monotonically increasing but not contiguous */
        uint16_le symbol_type;   /* type of the symbol. */
        uint16_le element_length; /* length of one array element in bytes. */
        uint32_le array_dims[3];  /* array dimensions. */
        uint16_le string_len;   /* string length count. */
        //uint8_t string_name[82]; /* MAGIC string name bytes (string_len of them, zero padded) */
} END_PACK tag_list_entry;
#endif


static int build_read_request_connected(ab_tag_p tag, int byte_offset);
//static int build_tag_list_request_connected(ab_tag_p tag);
static int build_read_request_unconnected(ab_tag_p tag, int byte_offset);
static int build_write_request_connected(ab_tag_p tag, int byte_offset);
static int build_write_request_unconnected(ab_tag_p tag, int byte_offset);
static int build_write_bit_request_connected(ab_tag_p tag);
static int build_write_bit_request_unconnected(ab_tag_p tag);
static int check_read_status_connected(ab_tag_p tag);
static int check_read_status_unconnected(ab_tag_p tag);
static int check_write_status_connected(ab_tag_p tag);
static int check_write_status_unconnected(ab_tag_p tag);
static int calculate_write_data_per_packet(ab_tag_p tag);

static int tag_read_start(ab_tag_p tag);
static int tag_tickler(ab_tag_p tag);
static int tag_write_start(ab_tag_p tag);

#if 0

/* define the exported vtable for this tag type. */
struct tag_vtable_t eip_cip_vtable = {
    (tag_vtable_func)ab_tag_abort, /* shared */
    (tag_vtable_func)tag_read_start,
    (tag_vtable_func)ab_tag_status, /* shared */
    (tag_vtable_func)tag_tickler,
    (tag_vtable_func)tag_write_start,
    (tag_vtable_func)NULL, /* wake_plc */

    /* attribute accessors */
    ab_get_int_attrib,
    ab_set_int_attrib
};

/* default string types used for ControlLogix-class PLCs. */
tag_byte_order_t logix_tag_byte_order = {
    .is_allocated = 0,

    .str_is_defined = 1,
    .str_is_counted = 1,
    .str_is_fixed_length = 1,
    .str_is_zero_terminated = 0,
    .str_is_byte_swapped = 0,

    .str_count_word_bytes = 4,
    .str_max_capacity = 82,
    .str_total_length = 88,
    .str_pad_bytes = 2,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};


/* default string types used for Omron-NJ/NX PLCs. */
tag_byte_order_t omron_njnx_tag_byte_order = {
    .is_allocated = 0,

    .str_is_defined = 1,
    .str_is_counted = 1,
    .str_is_fixed_length = 0,
    .str_is_zero_terminated = 1,
    .str_is_byte_swapped = 0,

    .str_count_word_bytes = 2,
    .str_max_capacity = 0,
    .str_total_length = 0,
    .str_pad_bytes = 0,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};

tag_byte_order_t logix_tag_listing_byte_order = {
    .is_allocated = 0,

    .str_is_defined = 1,
    .str_is_counted = 1,
    .str_is_fixed_length = 0,
    .str_is_zero_terminated = 0,
    .str_is_byte_swapped = 0,

    .str_count_word_bytes = 2,
    .str_max_capacity = 0,
    .str_total_length = 0,
    .str_pad_bytes = 0,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};

#endif

/*************************************************************************
 **************************** API Functions ******************************
 ************************************************************************/


int tag_tickler(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW,"Starting.");

    if (tag->read_in_progress) {
        if(tag->use_connected_msg) {
            rc = check_read_status_connected(tag);
        } else {
            rc = check_read_status_unconnected(tag);
        }

        tag->status = (int8_t)rc;

        /* if the operation completed, make a note so that the callback will be called. */
        if(!tag->read_in_progress) {
            /* done! */
            if(tag->first_read) {
                tag->first_read = 0;
                tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_CREATED, (int8_t)rc);
            }

            tag->read_complete = 1;
        }

        pdebug(DEBUG_SPEW,"Done.  Read in progress.");

        return rc;
    }

    if (tag->write_in_progress) {
        if(tag->use_connected_msg) {
            rc = check_write_status_connected(tag);
        } else {
            rc = check_write_status_unconnected(tag);
        }

        tag->status = (int8_t)rc;

        /* if the operation completed, make a note so that the callback will be called. */
        if(!tag->write_in_progress) {
            tag->write_complete = 1;
        }

        pdebug(DEBUG_SPEW, "Done. Write in progress.");

        return rc;
    }

    pdebug(DEBUG_SPEW, "Done.  No operation in progress.");

    return tag->status;
}





/*
 * tag_read_common_start
 *
 * This function must be called only from within one thread, or while
 * the tag's mutex is locked.
 *
 * The function starts the process of getting tag data from the PLC.
 */

int tag_read_start(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting");

    if(tag->read_in_progress || tag->write_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    /* mark the tag read in progress */
    tag->read_in_progress = 1;

    /* i is the index of the first new request */
    if(tag->use_connected_msg) {
        // if(tag->tag_list) {
        //     rc = build_tag_list_request_connected(tag);
        // } else {
            rc = build_read_request_connected(tag, tag->offset);
        // }
    } else {
        rc = build_read_request_unconnected(tag, tag->offset);
    }

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to build read request!");

        tag->read_in_progress = 0;

        return rc;
    }

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}




/*
 * tag_write_common_start
 *
 * This must be called from one thread alone, or while the tag mutex is
 * locked.
 *
 * The routine starts the process of writing to a tag.
 */

int tag_write_start(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting");

    if(tag->read_in_progress || tag->write_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    /* the write is now in flight */
    tag->write_in_progress = 1;

    /*
     * if the tag has not been read yet, read it.
     *
     * This gets the type data and sets up the request
     * buffers.
     */

    if (tag->first_read) {
        pdebug(DEBUG_DETAIL, "No read has completed yet, doing pre-read to get type information.");

        tag->pre_write_read = 1;
        tag->write_in_progress = 0; /* temporarily mask this off */

        return tag_read_start(tag);
    }

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to calculate write sizes!");
        tag->write_in_progress = 0;

        return rc;
    }

    if(tag->use_connected_msg) {
        rc = build_write_request_connected(tag, tag->offset);
    } else {
        rc = build_write_request_unconnected(tag, tag->offset);
    }

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to build write request!");
        tag->write_in_progress = 0;

        return rc;
    }

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}



int build_read_request_connected(ab_tag_p tag, int byte_offset)
{
    eip_cip_co_req* cip = NULL;
    uint8_t* data = NULL;
    ab_request_p req = NULL;
    int rc = PLCTAG_STATUS_OK;
    uint8_t read_cmd = AB_EIP_CMD_CIP_READ_FRAG;

    pdebug(DEBUG_INFO, "Starting.");

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    /* point the request struct at the buffer */
    cip = (eip_cip_co_req*)(req->data);

    /* point to the end of the struct */
    data = (req->data) + sizeof(eip_cip_co_req);

    /*
     * set up the embedded CIP read packet
     * The format is:
     *
     * uint8_t cmd
     * LLA formatted name
     * uint16_t # of elements to read
     */

    //embed_start = data;

    /* set up the CIP Read request */
    if(tag->plc_type == AB_PLC_OMRON_NJNX) {
        read_cmd = AB_EIP_CMD_CIP_READ;
    } else {
        read_cmd = AB_EIP_CMD_CIP_READ_FRAG;
    }

    *data = read_cmd;
    data++;

    /* copy the tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* add the count of elements to read. */
    *((uint16_le*)data) = h2le16((uint16_t)(tag->elem_count));
    data += sizeof(uint16_le);

    if (read_cmd == AB_EIP_CMD_CIP_READ_FRAG) {
        /* add the byte offset for this request */
        *((uint32_le*)data) = h2le32((uint32_t)byte_offset);
        data += sizeof(uint32_le);
    }

    /* now we go back and fill in the fields of the static part */

    /* encap fields */
    cip->encap_command = h2le16(AB_EIP_CONNECTED_SEND); /* ALWAYS 0x0070 Unconnected Send*/

    /* router timeout */
    cip->router_timeout = h2le16(1); /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    cip->cpf_item_count = h2le16(2);                 /* ALWAYS 2 */
    cip->cpf_cai_item_type = h2le16(AB_EIP_ITEM_CAI);/* ALWAYS 0x00A1 connected address item */
    cip->cpf_cai_item_length = h2le16(4);            /* ALWAYS 4, size of connection ID*/
    cip->cpf_cdi_item_type = h2le16(AB_EIP_ITEM_CDI);/* ALWAYS 0x00B1 - connected Data Item */
    cip->cpf_cdi_item_length = h2le16((uint16_t)(data - (uint8_t*)(&cip->cpf_conn_seq_num))); /* REQ: fill in with length of remaining data. */

    /* set the size of the request */
    req->request_size = (int)(data - (req->data));

    /* set the session so that we know what session the request is aiming at */
    //req->session = tag->session;

    req->allow_packing = tag->allow_packing;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}




int build_read_request_unconnected(ab_tag_p tag, int byte_offset)
{
    eip_cip_uc_req* cip;
    uint8_t* data;
    uint8_t* embed_start, *embed_end;
    ab_request_p req = NULL;
    int rc = PLCTAG_STATUS_OK;
    uint8_t read_cmd = AB_EIP_CMD_CIP_READ_FRAG;
    uint16_le tmp_uint16_le;

    pdebug(DEBUG_INFO, "Starting.");

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    /* point the request struct at the buffer */
    cip = (eip_cip_uc_req*)(req->data);

    /* point to the end of the struct */
    data = (req->data) + sizeof(eip_cip_uc_req);

    /*
     * set up the embedded CIP read packet
     * The format is:
     *
     * uint8_t cmd
     * LLA formatted name
     * uint16_t # of elements to read
     */

    embed_start = data;

    /* set up the CIP Read request */
    if(tag->plc_type == AB_PLC_OMRON_NJNX) {
        read_cmd = AB_EIP_CMD_CIP_READ;
    } else {
        read_cmd = AB_EIP_CMD_CIP_READ_FRAG;
    }

    *data = read_cmd;
    data++;

    /* copy the tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* add the count of elements to read. */
    tmp_uint16_le = h2le16((uint16_t)(tag->elem_count));
    mem_copy(data, &tmp_uint16_le, (int)(unsigned int)sizeof(tmp_uint16_le));
    data += sizeof(tmp_uint16_le);

    /* add the byte offset for this request */
    if(read_cmd == AB_EIP_CMD_CIP_READ_FRAG) {
        /* FIXME - this may not work on some proessors. */
        *((uint32_le*)data) = h2le32((uint32_t)byte_offset);
        data += sizeof(uint32_le);
    }

    /* mark the end of the embedded packet */
    embed_end = data;

    /* Now copy in the routing information for the embedded message */
    /*
     * routing information.  Format:
     *
     * uint8_t path_size in 16-bit words
     * uint8_t reserved/pad (zero)
     * uint8_t[...] path (padded to even number of bytes)
     */
    if(tag->session->conn_path_size > 0) {
        *data = (tag->session->conn_path_size) / 2; /* in 16-bit words */
        data++;
        *data = 0; /* reserved/pad */
        data++;
        mem_copy(data, tag->session->conn_path, tag->session->conn_path_size);
        data += tag->session->conn_path_size;
    }

    /* now we go back and fill in the fields of the static part */

    /* encap fields */
    cip->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND); /* ALWAYS 0x0070 Unconnected Send*/

    /* router timeout */
    cip->router_timeout = h2le16(1); /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    cip->cpf_item_count = h2le16(2);                  /* ALWAYS 2 */
    cip->cpf_nai_item_type = h2le16(AB_EIP_ITEM_NAI); /* ALWAYS 0 */
    cip->cpf_nai_item_length = h2le16(0);             /* ALWAYS 0 */
    cip->cpf_udi_item_type = h2le16(AB_EIP_ITEM_UDI); /* ALWAYS 0x00B2 - Unconnected Data Item */
    cip->cpf_udi_item_length = h2le16((uint16_t)(data - (uint8_t*)(&cip->cm_service_code))); /* REQ: fill in with length of remaining data. */

    /* CM Service Request - Connection Manager */
    cip->cm_service_code = AB_EIP_CMD_UNCONNECTED_SEND; /* 0x52 Unconnected Send */
    cip->cm_req_path_size = 2;                          /* 2, size in 16-bit words of path, next field */
    cip->cm_req_path[0] = 0x20;                         /* class */
    cip->cm_req_path[1] = 0x06;                         /* Connection Manager */
    cip->cm_req_path[2] = 0x24;                         /* instance */
    cip->cm_req_path[3] = 0x01;                         /* instance 1 */

    /* Unconnected send needs timeout information */
    cip->secs_per_tick = AB_EIP_SECS_PER_TICK; /* seconds per tick */
    cip->timeout_ticks = AB_EIP_TIMEOUT_TICKS; /* timeout = src_secs_per_tick * src_timeout_ticks */

    /* size of embedded packet */
    cip->uc_cmd_length = h2le16((uint16_t)(embed_end - embed_start));

    /* set the size of the request */
    req->request_size = (int)(data - (req->data));

    /* allow packing if the tag allows it. */
    req->allow_packing = tag->allow_packing;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}


int build_write_bit_request_connected(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_co_req* cip = NULL;
    uint8_t* data = NULL;
    ab_request_p req = NULL;
    int i;

    pdebug(DEBUG_INFO, "Starting.");

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    rc = calculate_write_data_per_packet(tag);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to calculate valid write data per packet!.  rc=%s", plc_tag_decode_error(rc));
        return rc;
    }

    if(tag->write_data_per_packet < (tag->size * 2) + 2) {  /* 2 masks plus a count word. */
        pdebug(DEBUG_ERROR,"Insufficient space to write bit masks!");
        return PLCTAG_ERR_TOO_SMALL;
    }

    cip = (eip_cip_co_req*)(req->data);

    /* point to the end of the struct */
    data = (req->data) + sizeof(eip_cip_co_req);

    /*
     * set up the embedded CIP read packet
     * The format is:
     *
     * uint8_t cmd
     * LLA formatted name
     * uint16_t # size of a mask element
     * OR mask
     * AND mask
     */

    /*
     * set up the CIP Read-Modify-Write request type.
     */
    *data = AB_EIP_CMD_CIP_RMW;
    data++;

    /* copy the tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* write an INT of the mask size. */
    *data = (uint8_t)(tag->elem_size & 0xFF); data++;
    *data = (uint8_t)((tag->elem_size >> 8) & 0xFF); data++;

    /* write the OR mask */
    for(i=0; i < tag->elem_size; i++) {
        if((tag->bit/8) == i) {
            uint8_t mask = (uint8_t)(1 << (tag->bit % 8));

            /* if the bit is set, then we want to mask it on. */
            if(tag->data[tag->bit / 8] & mask) {
                *data = mask;
            } else {
                *data = (uint8_t)0;
            }

            pdebug(DEBUG_DETAIL, "adding OR mask byte %d: %x", i, *data);

            data++;
        } else {
            /* this is not the data we care about. */
            *data = (uint8_t)0;

            pdebug(DEBUG_DETAIL, "adding OR mask byte %d: %x", i, *data);

            data++;
        }
    }

    /* write the AND mask */
    for(i=0; i < tag->elem_size; i++) {
        if((tag->bit / 8) == i) {
            uint8_t mask = (uint8_t)(1 << (tag->bit % 8));

            /* if the bit is set, then we want to _not_ mask it off. */
            if(tag->data[tag->bit / 8] & mask) {
                *data = (uint8_t)0xFF;
            } else {
                *data = (uint8_t)(~mask);
            }

            pdebug(DEBUG_DETAIL, "adding OR mask byte %d: %x", i, *data);

            data++;
        } else {
            /* this is not the data we care about. */
            *data = (uint8_t)0xFF;

            pdebug(DEBUG_DETAIL, "adding OR mask byte %d: %x", i, *data);

            data++;
        }
    }

    /* let the rest of the system know that the write is complete after this. */
    tag->offset = tag->size;

    /* now we go back and fill in the fields of the static part */

    /* encap fields */
    cip->encap_command = h2le16(AB_EIP_CONNECTED_SEND); /* ALWAYS 0x0070 Unconnected Send*/

    /* router timeout */
    cip->router_timeout = h2le16(1); /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    cip->cpf_item_count = h2le16(2);                 /* ALWAYS 2 */
    cip->cpf_cai_item_type = h2le16(AB_EIP_ITEM_CAI);/* ALWAYS 0x00A1 connected address item */
    cip->cpf_cai_item_length = h2le16(4);            /* ALWAYS 4, size of connection ID*/
    cip->cpf_cdi_item_type = h2le16(AB_EIP_ITEM_CDI);/* ALWAYS 0x00B1 - connected Data Item */
    cip->cpf_cdi_item_length = h2le16((uint16_t)(data - (uint8_t*)(&cip->cpf_conn_seq_num))); /* REQ: fill in with length of remaining data. */

    /* set the size of the request */
    req->request_size = (int)(data - (req->data));

    /* allow packing if the tag allows it. */
    req->allow_packing = tag->allow_packing;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}




int build_write_bit_request_unconnected(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_uc_req* cip = NULL;
    uint8_t* data = NULL;
    uint8_t *embed_start = NULL;
    uint8_t *embed_end = NULL;
    ab_request_p req = NULL;
    int i = 0;

    pdebug(DEBUG_INFO, "Starting.");

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    rc = calculate_write_data_per_packet(tag);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to calculate valid write data per packet!.  rc=%s", plc_tag_decode_error(rc));
        return rc;
    }

    if(tag->write_data_per_packet < (tag->size * 2) + 2) {  /* 2 masks plus a count word. */
        pdebug(DEBUG_ERROR,"Insufficient space to write bit masks!");
        return PLCTAG_ERR_TOO_SMALL;
    }

    cip = (eip_cip_uc_req*)(req->data);

    /* point to the end of the struct */
    data = (req->data) + sizeof(eip_cip_uc_req);

    embed_start = data;

    /*
     * set up the embedded CIP read packet
     * The format is:
     *
     * uint8_t cmd
     * LLA formatted name
     * uint16_t # size of a mask element
     * OR mask
     * AND mask
     */

    /*
     * set up the CIP Read-Modify-Write request type.
     */
    *data = AB_EIP_CMD_CIP_RMW;
    data++;

    /* copy the tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* write an INT of the mask size. */
    *data = (uint8_t)(tag->elem_size & 0xFF); data++;
    *data = (uint8_t)((tag->elem_size >> 8) & 0xFF); data++;

    /* write the OR mask */
    for(i=0; i < tag->elem_size; i++) {
        if((tag->bit/8) == i) {
            uint8_t mask = (uint8_t)(1 << (tag->bit % 8));

            /* if the bit is set, then we want to mask it on. */
            if(tag->data[tag->bit / 8] & mask) {
                *data = mask;
            } else {
                *data = (uint8_t)0;
            }

            pdebug(DEBUG_DETAIL, "adding OR mask byte %d: %x", i, *data);

            data++;
        } else {
            /* this is not the data we care about. */
            *data = (uint8_t)0;

            pdebug(DEBUG_DETAIL, "adding OR mask byte %d: %x", i, *data);

            data++;
        }
    }

    /* write the AND mask */
    for(i=0; i < tag->elem_size; i++) {
        if((tag->bit / 8) == i) {
            uint8_t mask = (uint8_t)(1 << (tag->bit % 8));

            /* if the bit is set, then we want to _not_ mask it off. */
            if(tag->data[tag->bit / 8] & mask) {
                *data = (uint8_t)0xFF;
            } else {
                *data = (uint8_t)(~mask);
            }

            pdebug(DEBUG_DETAIL, "adding OR mask byte %d: %x", i, *data);

            data++;
        } else {
            /* this is not the data we care about. */
            *data = (uint8_t)0xFF;

            pdebug(DEBUG_DETAIL, "adding OR mask byte %d: %x", i, *data);

            data++;
        }
    }

    /* let the rest of the system know that the write is complete after this. */
    tag->offset = tag->size;

    /* now we go back and fill in the fields of the static part */
    /* mark the end of the embedded packet */
    embed_end = data;

    /*
     * after the embedded packet, we need to tell the message router
     * how to get to the target device.
     */

    /* Now copy in the routing information for the embedded message */
    *data = (tag->session->conn_path_size) / 2; /* in 16-bit words */
    data++;
    *data = 0;
    data++;
    mem_copy(data, tag->session->conn_path, tag->session->conn_path_size);
    data += tag->session->conn_path_size;

    /* now fill in the rest of the structure. */

    /* encap fields */
    cip->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND); /* ALWAYS 0x006F Unconnected Send*/

    /* router timeout */
    cip->router_timeout = h2le16(1); /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    cip->cpf_item_count = h2le16(2);                  /* ALWAYS 2 */
    cip->cpf_nai_item_type = h2le16(AB_EIP_ITEM_NAI); /* ALWAYS 0 */
    cip->cpf_nai_item_length = h2le16(0);             /* ALWAYS 0 */
    cip->cpf_udi_item_type = h2le16(AB_EIP_ITEM_UDI); /* ALWAYS 0x00B2 - Unconnected Data Item */
    cip->cpf_udi_item_length = h2le16((uint16_t)(data - (uint8_t*)(&(cip->cm_service_code)))); /* REQ: fill in with length of remaining data. */

    /* CM Service Request - Connection Manager */
    cip->cm_service_code = AB_EIP_CMD_UNCONNECTED_SEND; /* 0x52 Unconnected Send */
    cip->cm_req_path_size = 2;                          /* 2, size in 16-bit words of path, next field */
    cip->cm_req_path[0] = 0x20;                         /* class */
    cip->cm_req_path[1] = 0x06;                         /* Connection Manager */
    cip->cm_req_path[2] = 0x24;                         /* instance */
    cip->cm_req_path[3] = 0x01;                         /* instance 1 */

    /* Unconnected send needs timeout information */
    cip->secs_per_tick = AB_EIP_SECS_PER_TICK; /* seconds per tick */
    cip->timeout_ticks = AB_EIP_TIMEOUT_TICKS; /* timeout = srd_secs_per_tick * src_timeout_ticks */

    /* size of embedded packet */
    cip->uc_cmd_length = h2le16((uint16_t)(embed_end - embed_start));

    /* set the size of the request */
    req->request_size = (int)(data - (req->data));

    /* allow packing if the tag allows it. */
    req->allow_packing = tag->allow_packing;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}







int build_write_request_connected(ab_tag_p tag, int byte_offset)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_co_req* cip = NULL;
    uint8_t* data = NULL;
    ab_request_p req = NULL;
    int multiple_requests = 0;
    int write_size = 0;

    pdebug(DEBUG_INFO, "Starting.");

    if(tag->is_bit) {
        return build_write_bit_request_connected(tag);
    }

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    rc = calculate_write_data_per_packet(tag);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to calculate valid write data per packet!.  rc=%s", plc_tag_decode_error(rc));
        return rc;
    }

    if(tag->write_data_per_packet < tag->size) {
        multiple_requests = 1;
    }

    if(multiple_requests && tag->plc_type == AB_PLC_OMRON_NJNX) {
        pdebug(DEBUG_WARN, "Tag too large for unfragmented request on Omron PLC!");
        return PLCTAG_ERR_TOO_LARGE;
    }

    cip = (eip_cip_co_req*)(req->data);

    /* point to the end of the struct */
    data = (req->data) + sizeof(eip_cip_co_req);

    /*
     * set up the embedded CIP read packet
     * The format is:
     *
     * uint8_t cmd
     * LLA formatted name
     * data type to write
     * uint16_t # of elements to write
     * data to write
     */

    /*
     * set up the CIP Read request type.
     * Different if more than one request.
     *
     * This handles a bug where attempting fragmented requests
     * does not appear to work with a single boolean.
     */
    *data = (multiple_requests) ? AB_EIP_CMD_CIP_WRITE_FRAG : AB_EIP_CMD_CIP_WRITE;
    data++;

    /* copy the tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* copy encoded type info */
    if (tag->encoded_type_info_size) {
        mem_copy(data, tag->encoded_type_info, tag->encoded_type_info_size);
        data += tag->encoded_type_info_size;
    } else {
        pdebug(DEBUG_WARN,"Data type unsupported!");
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* copy the item count, little endian */
    *((uint16_le*)data) = h2le16((uint16_t)(tag->elem_count));
    data += sizeof(uint16_le);

    if (multiple_requests) {
        /* put in the byte offset */
        *((uint32_le*)data) = h2le32((uint32_t)(byte_offset));
        data += sizeof(uint32_le);
    }

    /* how much data to write? */
    write_size = tag->size - tag->offset;

    if(write_size > tag->write_data_per_packet) {
        write_size = tag->write_data_per_packet;
    }

    /* now copy the data to write */
    mem_copy(data, tag->data + tag->offset, write_size);
    data += write_size;
    tag->offset += write_size;

    /* need to pad data to multiple of 16-bits */
    if (write_size & 0x01) {
        *data = 0;
        data++;
    }

    /* now we go back and fill in the fields of the static part */

    /* encap fields */
    cip->encap_command = h2le16(AB_EIP_CONNECTED_SEND); /* ALWAYS 0x0070 Unconnected Send*/

    /* router timeout */
    cip->router_timeout = h2le16(1); /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    cip->cpf_item_count = h2le16(2);                 /* ALWAYS 2 */
    cip->cpf_cai_item_type = h2le16(AB_EIP_ITEM_CAI);/* ALWAYS 0x00A1 connected address item */
    cip->cpf_cai_item_length = h2le16(4);            /* ALWAYS 4, size of connection ID*/
    cip->cpf_cdi_item_type = h2le16(AB_EIP_ITEM_CDI);/* ALWAYS 0x00B1 - connected Data Item */
    cip->cpf_cdi_item_length = h2le16((uint16_t)(data - (uint8_t*)(&cip->cpf_conn_seq_num))); /* REQ: fill in with length of remaining data. */

    /* set the size of the request */
    req->request_size = (int)(data - (req->data));

    /* allow packing if the tag allows it. */
    req->allow_packing = tag->allow_packing;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}




int build_write_request_unconnected(ab_tag_p tag, int byte_offset)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_uc_req* cip = NULL;
    uint8_t* data = NULL;
    uint8_t *embed_start = NULL;
    uint8_t *embed_end = NULL;
    ab_request_p req = NULL;
    int multiple_requests = 0;
    int write_size = 0;

    pdebug(DEBUG_INFO, "Starting.");

    if(tag->is_bit) {
        return build_write_bit_request_unconnected(tag);
    }

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    rc = calculate_write_data_per_packet(tag);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to calculate valid write data per packet!.  rc=%s", plc_tag_decode_error(rc));
        return rc;
    }

    if(tag->write_data_per_packet < tag->size) {
        multiple_requests = 1;
    }

    if(multiple_requests && tag->plc_type == AB_PLC_OMRON_NJNX) {
        pdebug(DEBUG_WARN, "Tag too large for unfragmented request on Omron PLC!");
        return PLCTAG_ERR_TOO_LARGE;
    }

    cip = (eip_cip_uc_req*)(req->data);

    /* point to the end of the struct */
    data = (req->data) + sizeof(eip_cip_uc_req);

    embed_start = data;

    /*
     * set up the embedded CIP read packet
     * The format is:
     *
     * uint8_t cmd
     * LLA formatted name
     * data type to write
     * uint16_t # of elements to write
     * data to write
     */

    /*
     * set up the CIP Read request type.
     * Different if more than one request.
     *
     * This handles a bug where attempting fragmented requests
     * does not appear to work with a single boolean.
     */
    *data = (multiple_requests) ? AB_EIP_CMD_CIP_WRITE_FRAG : AB_EIP_CMD_CIP_WRITE;
    data++;

    /* copy the tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* copy encoded type info */
    if (tag->encoded_type_info_size) {
        mem_copy(data, tag->encoded_type_info, tag->encoded_type_info_size);
        data += tag->encoded_type_info_size;
    } else {
        pdebug(DEBUG_WARN,"Data type unsupported!");
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* copy the item count, little endian */
    *((uint16_le*)data) = h2le16((uint16_t)(tag->elem_count));
    data += sizeof(uint16_le);

    if (multiple_requests) {
        /* put in the byte offset */
        *((uint32_le*)data) = h2le32((uint32_t)byte_offset);
        data += sizeof(uint32_le);
    }

    /* how much data to write? */
    write_size = tag->size - tag->offset;

    if(write_size > tag->write_data_per_packet) {
        write_size = tag->write_data_per_packet;
    }

    /* now copy the data to write */
    mem_copy(data, tag->data + tag->offset, write_size);
    data += write_size;
    tag->offset += write_size;

    /* need to pad data to multiple of 16-bits */
    if (write_size & 0x01) {
        *data = 0;
        data++;
    }

    /* now we go back and fill in the fields of the static part */
    /* mark the end of the embedded packet */
    embed_end = data;

    /*
     * after the embedded packet, we need to tell the message router
     * how to get to the target device.
     */

    /* Now copy in the routing information for the embedded message */
    *data = (tag->session->conn_path_size) / 2; /* in 16-bit words */
    data++;
    *data = 0;
    data++;
    mem_copy(data, tag->session->conn_path, tag->session->conn_path_size);
    data += tag->session->conn_path_size;

    /* now fill in the rest of the structure. */

    /* encap fields */
    cip->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND); /* ALWAYS 0x006F Unconnected Send*/

    /* router timeout */
    cip->router_timeout = h2le16(1); /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    cip->cpf_item_count = h2le16(2);                  /* ALWAYS 2 */
    cip->cpf_nai_item_type = h2le16(AB_EIP_ITEM_NAI); /* ALWAYS 0 */
    cip->cpf_nai_item_length = h2le16(0);             /* ALWAYS 0 */
    cip->cpf_udi_item_type = h2le16(AB_EIP_ITEM_UDI); /* ALWAYS 0x00B2 - Unconnected Data Item */
    cip->cpf_udi_item_length = h2le16((uint16_t)(data - (uint8_t*)(&(cip->cm_service_code)))); /* REQ: fill in with length of remaining data. */

    /* CM Service Request - Connection Manager */
    cip->cm_service_code = AB_EIP_CMD_UNCONNECTED_SEND; /* 0x52 Unconnected Send */
    cip->cm_req_path_size = 2;                          /* 2, size in 16-bit words of path, next field */
    cip->cm_req_path[0] = 0x20;                         /* class */
    cip->cm_req_path[1] = 0x06;                         /* Connection Manager */
    cip->cm_req_path[2] = 0x24;                         /* instance */
    cip->cm_req_path[3] = 0x01;                         /* instance 1 */

    /* Unconnected send needs timeout information */
    cip->secs_per_tick = AB_EIP_SECS_PER_TICK; /* seconds per tick */
    cip->timeout_ticks = AB_EIP_TIMEOUT_TICKS; /* timeout = srd_secs_per_tick * src_timeout_ticks */

    /* size of embedded packet */
    cip->uc_cmd_length = h2le16((uint16_t)(embed_end - embed_start));

    /* set the size of the request */
    req->request_size = (int)(data - (req->data));

    /* allow packing if the tag allows it. */
    req->allow_packing = tag->allow_packing;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}



/*
 * check_read_status_connected
 *
 * This routine checks for any outstanding requests and copies in data
 * that has arrived.  At the end of the request, it will clean up the request
 * buffers.  This is not thread-safe!  It should be called with the tag mutex
 * locked!
 */

static int check_read_status_connected(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_co_resp* cip_resp;
    uint8_t* data;
    uint8_t* data_end;
    int partial_data = 0;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_read_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Read request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    /* point to the data */
    cip_resp = (eip_cip_co_resp*)(request->data);

    /* point to the start of the data */
    data = (request->data) + sizeof(eip_cip_co_resp);

    /* point the end of the data */
    data_end = (request->data + le2h16(cip_resp->encap_length) + sizeof(eip_encap));

    /* check the status */
    do {
        ptrdiff_t payload_size = (data_end - data);

        if (le2h16(cip_resp->encap_command) != AB_EIP_CONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", cip_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (le2h32(cip_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(cip_resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        /*
         * FIXME
         *
         * It probably should not be necessary to check for both as setting the type to anything other
         * than fragmented is error-prone.
         */

        if (cip_resp->reply_service != (AB_EIP_CMD_CIP_READ_FRAG | AB_EIP_CMD_CIP_OK)
            && cip_resp->reply_service != (AB_EIP_CMD_CIP_READ | AB_EIP_CMD_CIP_OK) ) {
            pdebug(DEBUG_WARN, "CIP response reply service unexpected: %d", cip_resp->reply_service);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (cip_resp->status != AB_CIP_STATUS_OK && cip_resp->status != AB_CIP_STATUS_FRAG) {
            pdebug(DEBUG_WARN, "CIP read failed with status: 0x%x %s", cip_resp->status, decode_cip_error_short((uint8_t *)&cip_resp->status));
            pdebug(DEBUG_INFO, decode_cip_error_long((uint8_t *)&cip_resp->status));

            rc = decode_cip_error_code((uint8_t *)&cip_resp->status);

            break;
        }

        /* check to see if this is a partial response. */
        partial_data = (cip_resp->status == AB_CIP_STATUS_FRAG);

        /*
         * check to see if there is any data to process.  If this is a packed
         * response, there might not be.
         */
        payload_size = (data_end - data);
        if(payload_size > 0) {
            /* the first byte of the response is a type byte. */
            pdebug(DEBUG_DETAIL, "type byte = %d (%x)", (int)*data, (int)*data);

            /* handle the data type part.  This can be long. */

            /* check for a simple/base type */
            if ((*data) >= AB_CIP_DATA_BIT && (*data) <= AB_CIP_DATA_STRINGI) {
                /* copy the type info for later. */
                if (tag->encoded_type_info_size == 0) {
                    tag->encoded_type_info_size = 2;
                    mem_copy(tag->encoded_type_info, data, tag->encoded_type_info_size);
                }

                /* skip the type byte and zero length byte */
                data += 2;
            } else if ((*data) == AB_CIP_DATA_ABREV_STRUCT || (*data) == AB_CIP_DATA_ABREV_ARRAY ||
                       (*data) == AB_CIP_DATA_FULL_STRUCT || (*data) == AB_CIP_DATA_FULL_ARRAY) {
                /* this is an aggregate type of some sort, the type info is variable length */
                int type_length = *(data + 1) + 2;  /*
                                                       * MAGIC
                                                       * add 2 to get the total length including
                                                       * the type byte and the length byte.
                                                       */

                /* check for extra long types */
                if (type_length > MAX_TAG_TYPE_INFO) {
                    pdebug(DEBUG_WARN, "Read data type info is too long (%d)!", type_length);
                    rc = PLCTAG_ERR_TOO_LARGE;
                    break;
                }

                /* copy the type info for later. */
                if (tag->encoded_type_info_size == 0) {
                    tag->encoded_type_info_size = type_length;
                    mem_copy(tag->encoded_type_info, data, tag->encoded_type_info_size);
                }

                data += type_length;
            } else {
                pdebug(DEBUG_WARN, "Unsupported data type returned, type byte=%d", *data);
                rc = PLCTAG_ERR_UNSUPPORTED;
                break;
            }

            /* check payload size now that we have bumped past the data type info. */
            payload_size = (data_end - data);

            /* copy the data into the tag and realloc if we need more space. */
            if(payload_size + tag->offset > tag->size) {
                tag->size = (int)payload_size + tag->offset;
                tag->elem_size = tag->size / tag->elem_count;

                pdebug(DEBUG_DETAIL, "Increasing tag buffer size to %d bytes.", tag->size);

                tag->data = (uint8_t*)mem_realloc(tag->data, tag->size);
                if(!tag->data) {
                    pdebug(DEBUG_WARN, "Unable to reallocate tag data memory!");
                    rc = PLCTAG_ERR_NO_MEM;
                    break;
                }
            }

            pdebug(DEBUG_INFO, "Got %d bytes of data", (int)payload_size);

            /*
             * copy the data, but only if this is not
             * a pre-read for a subsequent write!  We do not
             * want to overwrite the data the upstream has
             * put into the tag's data buffer.
             */
            if (!tag->pre_write_read) {
                mem_copy(tag->data + tag->offset, data, (int)(payload_size));
            }

            /* bump the byte offset */
            tag->offset += (int)(payload_size);
        } else {
            pdebug(DEBUG_DETAIL, "Response returned no data and no error.");
        }

        /* set the return code */
        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    /* are we actually done? */
    if (rc == PLCTAG_STATUS_OK) {
        /* this particular read is done. */
        tag->read_in_progress = 0;

        /* skip if we are doing a pre-write read. */
        if (!tag->pre_write_read && partial_data) {
            /* call read start again to get the next piece */
            pdebug(DEBUG_DETAIL, "calling tag_read_start() to get the next chunk.");
            rc = tag_read_start(tag);
        } else {
            tag->offset = 0;

            /* if this is a pre-read for a write, then pass off to the write routine */
            if (tag->pre_write_read) {
                pdebug(DEBUG_DETAIL, "Restarting write call now.");
                tag->pre_write_read = 0;
                rc = tag_write_start(tag);
            }
        }
    }

    /* this is not an else clause because the above if could result in bad rc. */
    if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
        /* error ! */
        pdebug(DEBUG_WARN, "Error received!");

        /* clean up everything. */
        ab_tag_abort(tag);
    }

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}



static int check_read_status_unconnected(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_uc_resp* cip_resp;
    uint8_t* data;
    uint8_t* data_end;
    int partial_data = 0;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_read_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Read request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    /* point to the data */
    cip_resp = (eip_cip_uc_resp*)(request->data);

    /* point to the start of the data */
    data = (request->data) + sizeof(eip_cip_uc_resp);

    /* point the end of the data */
    data_end = (request->data + le2h16(cip_resp->encap_length) + sizeof(eip_encap));

    /* check the status */
    do {
        ptrdiff_t payload_size = (data_end - data);

        if (le2h16(cip_resp->encap_command) != AB_EIP_UNCONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", cip_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (le2h32(cip_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(cip_resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        /*
         * TODO
         *
         * It probably should not be necessary to check for both as setting the type to anything other
         * than fragmented is error-prone.
         */

        if (cip_resp->reply_service != (AB_EIP_CMD_CIP_READ_FRAG | AB_EIP_CMD_CIP_OK)
            && cip_resp->reply_service != (AB_EIP_CMD_CIP_READ | AB_EIP_CMD_CIP_OK) ) {
            pdebug(DEBUG_WARN, "CIP response reply service unexpected: %d", cip_resp->reply_service);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (cip_resp->status != AB_CIP_STATUS_OK && cip_resp->status != AB_CIP_STATUS_FRAG) {
            pdebug(DEBUG_WARN, "CIP read failed with status: 0x%x %s", cip_resp->status, decode_cip_error_short((uint8_t *)&cip_resp->status));
            pdebug(DEBUG_INFO, decode_cip_error_long((uint8_t *)&cip_resp->status));

            rc = decode_cip_error_code((uint8_t *)&cip_resp->status);

            break;
        }

        /* check to see if this is a partial response. */
        partial_data = (cip_resp->status == AB_CIP_STATUS_FRAG);

        /* the first byte of the response is a type byte. */
        pdebug(DEBUG_DETAIL, "type byte = %d (%x)", (int)*data, (int)*data);

        /* copy the type data. */

        /* check for a simple/base type */

        if ((*data) >= AB_CIP_DATA_BIT && (*data) <= AB_CIP_DATA_STRINGI) {
            /* copy the type info for later. */
            if (tag->encoded_type_info_size == 0) {
                tag->encoded_type_info_size = 2;
                mem_copy(tag->encoded_type_info, data, tag->encoded_type_info_size);
            }

            /* skip the type byte and zero length byte */
            data += 2;
        } else if ((*data) == AB_CIP_DATA_ABREV_STRUCT || (*data) == AB_CIP_DATA_ABREV_ARRAY ||
                   (*data) == AB_CIP_DATA_FULL_STRUCT || (*data) == AB_CIP_DATA_FULL_ARRAY) {
            /* this is an aggregate type of some sort, the type info is variable length */
            int type_length =
                *(data + 1) + 2;  /*
                                   * MAGIC
                                   * add 2 to get the total length including
                                   * the type byte and the length byte.
                                   */

            /* check for extra long types */
            if (type_length > MAX_TAG_TYPE_INFO) {
                pdebug(DEBUG_WARN, "Read data type info is too long (%d)!", type_length);
                rc = PLCTAG_ERR_TOO_LARGE;
                break;
            }

            /* copy the type info for later. */
            if (tag->encoded_type_info_size == 0) {
                tag->encoded_type_info_size = type_length;
                mem_copy(tag->encoded_type_info, data, tag->encoded_type_info_size);
            }

            data += type_length;
        } else {
            pdebug(DEBUG_WARN, "Unsupported data type returned, type byte=%d", *data);
            rc = PLCTAG_ERR_UNSUPPORTED;
            break;
        }

        /* check payload size now that we have bumped past the data type info. */
        payload_size = (data_end - data);

        /* copy the data into the tag and realloc if we need more space. */
        if(payload_size + tag->offset > tag->size) {
            tag->size = (int)payload_size + tag->offset;
            tag->elem_size = tag->size / tag->elem_count;

            pdebug(DEBUG_DETAIL, "Increasing tag buffer size to %d bytes.", tag->size);

            tag->data = (uint8_t*)mem_realloc(tag->data, tag->size);
            if(!tag->data) {
                pdebug(DEBUG_WARN, "Unable to reallocate tag data memory!");
                rc = PLCTAG_ERR_NO_MEM;
                break;
            }
        }

        pdebug(DEBUG_INFO, "Got %d bytes of data", (int)payload_size);


        /*
         * copy the data, but only if this is not
         * a pre-read for a subsequent write!  We do not
         * want to overwrite the data the upstream has
         * put into the tag's data buffer.
         */
        if (!tag->pre_write_read) {
            mem_copy(tag->data + tag->offset, data, (int)payload_size);
        }

        /* bump the byte offset */
        tag->offset += (int)payload_size;

        /* set the return code */
        rc = PLCTAG_STATUS_OK;
    } while(0);


    /* clean up the request */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    /* are we actually done? */
    if (rc == PLCTAG_STATUS_OK) {
        /* this read is done. */
        tag->read_in_progress = 0;

        /* skip if we are doing a pre-write read. */
        if (!tag->pre_write_read && partial_data && tag->offset < tag->size) {
            /* call read start again to get the next piece */
            pdebug(DEBUG_DETAIL, "calling tag_read_start() to get the next chunk.");
            rc = tag_read_start(tag);
        } else {
            tag->offset = 0;

            /* if this is a pre-read for a write, then pass off to the write routine */
            if (tag->pre_write_read) {
                pdebug(DEBUG_DETAIL, "Restarting write call now.");

                tag->pre_write_read = 0;
                rc = tag_write_start(tag);
            }
        }
    }

    /* this is not an else clause because the above if could result in bad rc. */
    if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
        /* error ! */
        pdebug(DEBUG_WARN, "Error received!");

        /* clean up everything. */
        ab_tag_abort(tag);
    }

    /* release the referene to the request. */
    rc_dec(request);

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}




/*
 * check_write_status_connected
 *
 * This routine must be called with the tag mutex locked.  It checks the current
 * status of a write operation.  If the write is done, it triggers the clean up.
 */

static int check_write_status_connected(ab_tag_p tag)
{
    eip_cip_co_resp* cip_resp;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_write_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Write request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    /* point to the data */
    cip_resp = (eip_cip_co_resp*)(request->data);

    do {
        if (le2h16(cip_resp->encap_command) != AB_EIP_CONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", cip_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (le2h32(cip_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(cip_resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if (cip_resp->reply_service != (AB_EIP_CMD_CIP_WRITE_FRAG | AB_EIP_CMD_CIP_OK)
            && cip_resp->reply_service != (AB_EIP_CMD_CIP_WRITE | AB_EIP_CMD_CIP_OK)
            && cip_resp->reply_service != (AB_EIP_CMD_CIP_RMW | AB_EIP_CMD_CIP_OK)) {
            pdebug(DEBUG_WARN, "CIP response reply service unexpected: %d", cip_resp->reply_service);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (cip_resp->status != AB_CIP_STATUS_OK && cip_resp->status != AB_CIP_STATUS_FRAG) {
            pdebug(DEBUG_WARN, "CIP read failed with status: 0x%x %s", cip_resp->status, decode_cip_error_short((uint8_t *)&cip_resp->status));
            pdebug(DEBUG_INFO, decode_cip_error_long((uint8_t *)&cip_resp->status));
            rc = decode_cip_error_code((uint8_t *)&cip_resp->status);
            break;
        }
    } while(0);

    /* clean up the request. */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    /* write is done in one way or another. */
    tag->write_in_progress = 0;

    if(rc == PLCTAG_STATUS_OK) {
        if(tag->offset < tag->size) {

            pdebug(DEBUG_DETAIL, "Write not complete, triggering next round.");
            rc = tag_write_start(tag);
        } else {
            /* only clear this if we are done. */
            tag->offset = 0;
        }
    } else {
        pdebug(DEBUG_WARN,"Write failed!");

        tag->offset = 0;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}





static int check_write_status_unconnected(ab_tag_p tag)
{
    eip_cip_uc_resp* cip_resp;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_write_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Write request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    /* point to the data */
    cip_resp = (eip_cip_uc_resp*)(request->data);

    do {
        if (le2h16(cip_resp->encap_command) != AB_EIP_CONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", cip_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (le2h32(cip_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(cip_resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if (cip_resp->reply_service != (AB_EIP_CMD_CIP_WRITE_FRAG | AB_EIP_CMD_CIP_OK)
            && cip_resp->reply_service != (AB_EIP_CMD_CIP_WRITE | AB_EIP_CMD_CIP_OK)
            && cip_resp->reply_service != (AB_EIP_CMD_CIP_RMW | AB_EIP_CMD_CIP_OK)) {
            pdebug(DEBUG_WARN, "CIP response reply service unexpected: %d", cip_resp->reply_service);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }


        if (cip_resp->status != AB_CIP_STATUS_OK && cip_resp->status != AB_CIP_STATUS_FRAG) {
            pdebug(DEBUG_WARN, "CIP read failed with status: 0x%x %s", cip_resp->status, decode_cip_error_short((uint8_t *)&cip_resp->status));
            pdebug(DEBUG_INFO, decode_cip_error_long((uint8_t *)&cip_resp->status));
            rc = decode_cip_error_code((uint8_t *)&cip_resp->status);
            break;
        }
    } while(0);

    /* clean up the request. */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    /* write is done in one way or another */
    tag->write_in_progress = 0;

    if(rc == PLCTAG_STATUS_OK) {
        if(tag->offset < tag->size) {

            pdebug(DEBUG_DETAIL, "Write not complete, triggering next round.");
            rc = tag_write_start(tag);
        } else {
            /* only clear this if we are done. */
            tag->offset = 0;
        }
    } else {
        pdebug(DEBUG_WARN,"Write failed!");
        tag->offset = 0;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}




int calculate_write_data_per_packet(ab_tag_p tag)
{
    int overhead = 0;
    int data_per_packet = 0;
    int max_payload_size = 0;

    pdebug(DEBUG_DETAIL, "Starting.");

    /* if we are here, then we have all the type data etc. */
    if(tag->use_connected_msg) {
        pdebug(DEBUG_DETAIL,"Connected tag.");
        max_payload_size = session_get_max_payload(tag->session);
        overhead =  1                               /* service request, one byte */
                    + tag->encoded_name_size        /* full encoded name */
                    + tag->encoded_type_info_size   /* encoded type size */
                    + 2                             /* element count, 16-bit int */
                    + 4                             /* byte offset, 32-bit int */
                    + 8;                            /* MAGIC fudge factor */
    } else {
        pdebug(DEBUG_DETAIL,"Unconnected tag.");
        max_payload_size = session_get_max_payload(tag->session);
        overhead =  1                               /* service request, one byte */
                    + tag->encoded_name_size        /* full encoded name */
                    + tag->encoded_type_info_size   /* encoded type size */
                    + tag->session->conn_path_size + 2       /* encoded device path size plus two bytes for length and padding */
                    + 2                             /* element count, 16-bit int */
                    + 4                             /* byte offset, 32-bit int */
                    + 8;                            /* MAGIC fudge factor */
    }

    data_per_packet = max_payload_size - overhead;

    pdebug(DEBUG_DETAIL,"Write packet maximum size is %d, write overhead is %d, and write data per packet is %d.", max_payload_size, overhead, data_per_packet);

    if (data_per_packet <= 0) {
        pdebug(DEBUG_WARN,
               "Unable to send request.  Packet overhead, %d bytes, is too large for packet, %d bytes!",
               overhead,
               max_payload_size);
        return PLCTAG_ERR_TOO_LARGE;
    }

    /* we want a multiple of 8 bytes */
    data_per_packet &= 0xFFFFF8;

    tag->write_data_per_packet = data_per_packet;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}


#endif // __PROTOCOLS_AB_EIP_CIP_C__


#ifndef __PROTOCOLS_AB_EIP_LGX_PCCC_C__
#define __PROTOCOLS_AB_EIP_LGX_PCCC_C__

START_PACK typedef struct {
    /* PCCC Command Req Routing */
    uint8_t service_code;           /* ALWAYS 0x4B, Execute PCCC */
    uint8_t req_path_size;          /* ALWAYS 0x02, in 16-bit words */
    uint8_t req_path[4];            /* ALWAYS 0x20,0x67,0x24,0x01 for PCCC */
    uint8_t request_id_size;        /* ALWAYS 7 */
    uint16_le vendor_id;             /* Our CIP Vendor ID */
    uint32_le vendor_serial_number;  /* Our CIP Vendor Serial Number */
    /* PCCC Command */
    uint8_t pccc_command;           /* CMD read, write etc. */
    uint8_t pccc_status;            /* STS 0x00 in request */
    uint16_le pccc_seq_num;          /* TNS transaction/sequence id */
    uint8_t pccc_function;          /* FNC sub-function of command */
    uint16_le pccc_offset;           /* offset of requested in total request */
    uint16_le pccc_transfer_size;    /* total number of words requested */
} END_PACK embedded_pccc;


static int tag_read_start(ab_tag_p tag);
static int tag_status(ab_tag_p tag);
static int tag_tickler(ab_tag_p tag);
static int tag_write_start(ab_tag_p tag);

struct tag_vtable_t lgx_pccc_vtable = {
    (tag_vtable_func)ab_tag_abort, /* shared */
    (tag_vtable_func)tag_read_start,
    (tag_vtable_func)tag_status,
    (tag_vtable_func)tag_tickler,
    (tag_vtable_func)tag_write_start,
    (tag_vtable_func)NULL, /* wake_plc */

    /* data accessors */
    ab_get_int_attrib,
    ab_set_int_attrib
};

static int check_read_status(ab_tag_p tag);
static int check_write_status(ab_tag_p tag);

/*
 * tag_status
 *
 * CIP/PCCC-specific status.
 */
int tag_status(ab_tag_p tag)
{
    if (!tag->session) {
        /* this is not OK.  This is fatal! */
        return PLCTAG_ERR_CREATE;
    }

    if(tag->read_in_progress) {
        return PLCTAG_STATUS_PENDING;
    }

    if(tag->write_in_progress) {
        return PLCTAG_STATUS_PENDING;
    }

    return tag->status;
}




int tag_tickler(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW, "Starting.");

    if(tag->read_in_progress) {
        pdebug(DEBUG_SPEW, "Read in progress.");
        rc = check_read_status(tag);
        tag->status = (int8_t)rc;

        /* check to see if the read finished. */
        if(!tag->read_in_progress) {
            /* read done */
            if(tag->first_read) {
                tag->first_read = 0;
                tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_CREATED, PLCTAG_STATUS_OK);
            }

            tag->read_complete = 1;
        }

        return rc;
    }

    if(tag->write_in_progress) {
        pdebug(DEBUG_SPEW, "Write in progress.");
        rc = check_write_status(tag);
        tag->status = (int8_t)rc;

        /* check to see if the write finished. */
        if(!tag->write_in_progress) {
            tag->write_complete = 1;
        }

        return rc;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return tag->status;

}




/*
 * tag_read_start
 *
 * Start a PCCC tag read (PLC5, SLC).
 */

int tag_read_start(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    ab_request_p req;
    uint16_t conn_seq_id = (uint16_t)(session_get_new_seq_id(tag->session));;
    int overhead;
    int data_per_packet;
    eip_cip_uc_req *lgx_pccc;
    embedded_pccc *embed_pccc;
    uint8_t *data;
    uint8_t *embed_start;

    pdebug(DEBUG_INFO,"Starting");

    if(tag->read_in_progress || tag->write_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    tag->read_in_progress = 1;

    /* calculate based on the response. */
    overhead =   1      /* reply code */
                 +1      /* reserved */
                 +1      /* general status */
                 +1      /* status size */
                 +1      /* request ID size */
                 +2      /* vendor ID */
                 +4      /* vendor serial number */
                 +1      /* PCCC command */
                 +1      /* PCCC status */
                 +2      /* PCCC sequence number */
                 +1      /* type byte */
                 +2      /* maximum extended type. */
                 +2      /* maximum extended size. */
                 +1      /* secondary type byte if type was array. */
                 +2      /* maximum extended type. */
                 +2;     /* maximum extended size. */

    /* TODO - this is not correct for this kind of transaction */

    data_per_packet = session_get_max_payload(tag->session) - overhead;

    if(data_per_packet <= 0) {
        tag->read_in_progress = 0;
        pdebug(DEBUG_WARN,"Unable to send request.  Packet overhead, %d bytes, is too large for packet, %d bytes!", overhead, session_get_max_payload(tag->session));
        return PLCTAG_ERR_TOO_LARGE;
    }

    if(data_per_packet < tag->size) {
        tag->read_in_progress = 0;
        pdebug(DEBUG_DETAIL,"Tag size is %d, write overhead is %d, and write data per packet is %d.", tag->size, overhead, data_per_packet);
        return PLCTAG_ERR_TOO_LARGE;
    }

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);

    if(rc != PLCTAG_STATUS_OK) {
        tag->read_in_progress = 0;
        pdebug(DEBUG_WARN,"Unable to get new request.  rc=%d",rc);
        return rc;
    }

    /* point the struct pointers to the buffer*/
    lgx_pccc = (eip_cip_uc_req *)(req->data);
    embed_pccc = (embedded_pccc *)(lgx_pccc + 1);

    /* set up the embedded PCCC packet */
    embed_start = (uint8_t *)(embed_pccc);

    /* Command Routing */
    embed_pccc->service_code = AB_EIP_CMD_PCCC_EXECUTE;  /* ALWAYS 0x4B, Execute PCCC */
    embed_pccc->req_path_size = 2;   /* ALWAYS 2, size in words of path, next field */
    embed_pccc->req_path[0] = 0x20;  /* class */
    embed_pccc->req_path[1] = 0x67;  /* PCCC Execute */
    embed_pccc->req_path[2] = 0x24;  /* instance */
    embed_pccc->req_path[3] = 0x01;  /* instance 1 */

    /* Vendor info */
    embed_pccc->request_id_size = 7;  /* ALWAYS 7 */
    embed_pccc->vendor_id = h2le16(AB_EIP_VENDOR_ID);             /* Our CIP Vendor */
    embed_pccc->vendor_serial_number = h2le32(AB_EIP_VENDOR_SN);      /* our unique serial number */

    /* fill in the PCCC command */
    embed_pccc->pccc_command = AB_EIP_PCCC_TYPED_CMD;
    embed_pccc->pccc_status = 0;  /* STS 0 in request */
    embed_pccc->pccc_seq_num = h2le16(conn_seq_id); /* TODO - get sequence ID from session? */
    embed_pccc->pccc_function = AB_EIP_PCCCLGX_TYPED_READ_FUNC;
    embed_pccc->pccc_offset = h2le16((uint16_t)0);
    embed_pccc->pccc_transfer_size = h2le16((uint16_t)tag->elem_count); /* This is the offset items */

    /* point to the end of the struct */
    data = (uint8_t *)(embed_pccc + 1);

    /* copy encoded tag name into the request */
    mem_copy(data,tag->encoded_name,tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* FIXME - This is the total items, and this is unsafe on some processors */
    *((uint16_le *)data) = h2le16((uint16_t)tag->elem_count); /* elements */
    data += sizeof(uint16_le);

    /* if this is not an multiple of 16-bit chunks, pad it out */
    if((data - embed_start) & 0x01) {
        *data = 0;
        data++;
    }

    /*
     * after the embedded packet, we need to tell the message router
     * how to get to the target device.
     */

    /* encap fields */
    lgx_pccc->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND);    /* set up for unconnected sending */

    /* router timeout */
    lgx_pccc->router_timeout = h2le16(1);                 /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    lgx_pccc->cpf_item_count        = h2le16(2);                /* ALWAYS 2 */
    lgx_pccc->cpf_nai_item_type     = h2le16(AB_EIP_ITEM_NAI);  /* ALWAYS 0 */
    lgx_pccc->cpf_nai_item_length   = h2le16(0);                /* ALWAYS 0 */
    lgx_pccc->cpf_udi_item_type     = h2le16(AB_EIP_ITEM_UDI);  /* ALWAYS 0x00B2 - Unconnected Data Item */

    lgx_pccc->cm_service_code       = AB_EIP_CMD_UNCONNECTED_SEND;     /* unconnected send */
    lgx_pccc->cm_req_path_size      = 0x02;     /* path size in words */
    lgx_pccc->cm_req_path[0]        = 0x20;     /* class */
    lgx_pccc->cm_req_path[1]        = 0x06;     /* Connection Manager */
    lgx_pccc->cm_req_path[2]        = 0x24;     /* instance */
    lgx_pccc->cm_req_path[3]        = 0x01;     /* instance 1 */

    /* Unconnected send needs timeout information */
    lgx_pccc->secs_per_tick = AB_EIP_SECS_PER_TICK; /* seconds per tick */
    lgx_pccc->timeout_ticks = AB_EIP_TIMEOUT_TICKS; /* timeout = src_secs_per_tick * src_timeout_ticks */

    /* how big is the embedded packet? */
    lgx_pccc->uc_cmd_length = h2le16((uint16_t)(data - embed_start));

    /* copy the path */
    if(tag->session->conn_path_size > 0) {
        *data = (tag->session->conn_path_size) / 2; /* in 16-bit words */
        data++;
        *data = 0; /* reserved/pad */
        data++;
        mem_copy(data, tag->session->conn_path, tag->session->conn_path_size);
        data += tag->session->conn_path_size;
    } else {
        pdebug(DEBUG_DETAIL, "connection path is of length %d!", tag->session->conn_path_size);
    }

    /* how big is the unconnected data item? */
    lgx_pccc->cpf_udi_item_length   = h2le16((uint16_t)(data - (uint8_t *)(&lgx_pccc->cm_service_code)));

    /* set the size of the request */
    req->request_size = (int)(data - (req->data));

    /* mark it as ready to send */
    //req->send_request = 1;
    req->allow_packing = tag->allow_packing;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        tag->read_in_progress = 0;

        return rc;
    }

    /* save the request for later */
    tag->req = req;
    req = NULL;

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}




/*
 * check_read_status
 *
 * NOTE that we can have only one outstanding request because PCCC
 * does not support fragments.
 */


static int check_read_status(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW,"Starting");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_read_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Read request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    /* fake exceptions */
    do {
        pccc_resp *pccc;
        uint8_t *data;
        uint8_t *data_end;
        uint8_t *type_start;
        uint8_t *type_end;
        int pccc_res_type;
        int pccc_res_length;

        pccc = (pccc_resp *)(request->data);

        /* point to the start of the data */
        data = (uint8_t *)pccc + sizeof(*pccc);

        data_end = (request->data + le2h16(pccc->encap_length) + sizeof(eip_encap));

        if(le2h16(pccc->encap_command) != AB_EIP_UNCONNECTED_SEND) {
            pdebug(DEBUG_WARN,"Unexpected EIP packet type received: %d!",pccc->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if(le2h32(pccc->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN,"EIP command failed, response code: %d",le2h32(pccc->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc->general_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN,"PCCC command failed, response code: (%d) %s", pccc->general_status, decode_cip_error_long((uint8_t *)&(pccc->general_status)));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc->pccc_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "PCCC command failed, response code: %d - %s", pccc->pccc_status, pccc_decode_error(&pccc->pccc_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        type_start = data;

        if(!(data = pccc_decode_dt_byte(data, (int)(data_end - data), &pccc_res_type,&pccc_res_length))) {
            pdebug(DEBUG_WARN,"Unable to decode PCCC response data type and data size!");
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        /* this gives us the overall type of the response and the number of bytes remaining in it.
         * If the type is an array, then we need to decode another one of these words
         * to get the type of each element and the size of each element.  We will
         * need to adjust the size if we care.
         */

        if(pccc_res_type == AB_PCCC_DATA_ARRAY) {
            if(!(data = pccc_decode_dt_byte(data, (int)(data_end - data), &pccc_res_type,&pccc_res_length))) {
                pdebug(DEBUG_WARN,"Unable to decode PCCC response array element data type and data size!");
                rc = PLCTAG_ERR_BAD_DATA;
                break;
            }
        }

        type_end = data;

        /* copy data into the tag. */
        if((data_end - data) > tag->size) {
            rc = PLCTAG_ERR_TOO_LARGE;
            break;
        }

        /*
         * skip this if this is a pre-read.
         * Otherwise this will overwrite the values
         * the user has set, possibly.
         */
        if(!tag->pre_write_read) {
            mem_copy(tag->data, data, (int)(data_end - data));
        }

        /* copy type data into tag. */
        tag->encoded_type_info_size = (int)(type_end - type_start);
        mem_copy(tag->encoded_type_info, type_start, tag->encoded_type_info_size);

        /* have the IO thread take care of the request buffers */
        ab_tag_abort(tag);

        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    tag->read_in_progress = 0;

    /* if this is a pre-read for a write, then pass off the the write routine */
    if (rc == PLCTAG_STATUS_OK && tag->pre_write_read) {
        pdebug(DEBUG_DETAIL, "Restarting write call now.");

        tag->pre_write_read = 0;
        rc = tag_write_start(tag);
    }

    pdebug(DEBUG_SPEW,"Done.");

    return rc;
}





int tag_write_start(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_uc_req *lgx_pccc;
    embedded_pccc *embed_pccc;
    uint8_t *data;
    uint16_t conn_seq_id = (uint16_t)(session_get_new_seq_id(tag->session));;
    ab_request_p req = NULL;
    uint8_t *embed_start;
    int overhead, data_per_packet;

    pdebug(DEBUG_INFO,"Starting.");

    if(tag->read_in_progress || tag->write_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    tag->write_in_progress = 1;

    if (tag->first_read) {
        pdebug(DEBUG_DETAIL, "No read has completed yet, doing pre-read to get type information.");

        tag->pre_write_read = 1;
        tag->write_in_progress = 0;

        return tag_read_start(tag);
    }

    /* overhead comes from the request in this case */
    overhead =   1  /* CIP PCCC command */
                 +2  /* path size for PCCC command */
                 +4  /* path to PCCC command object */
                 +1  /* request ID size */
                 +2  /* vendor ID */
                 +4  /* vendor serial number */
                 +1  /* PCCC command */
                 +1  /* PCCC status */
                 +2  /* PCCC sequence number */
                 +1  /* PCCC function */
                 +2  /* request offset */
                 +2  /* request total transfer size in elements. */
                 + (tag->encoded_name_size)
                 +2; /* actual request size in elements */

    data_per_packet = session_get_max_payload(tag->session) - overhead;

    if(data_per_packet <= 0) {
        tag->write_in_progress = 0;
        pdebug(DEBUG_WARN,"Unable to send request.  Packet overhead, %d bytes, is too large for packet, %d bytes!", overhead, session_get_max_payload(tag->session));
        return PLCTAG_ERR_TOO_LARGE;
    }

    if(data_per_packet < tag->size) {
        tag->write_in_progress = 0;
        pdebug(DEBUG_DETAIL,"Tag size is %d, write overhead is %d, and write data per packet is %d.", session_get_max_payload(tag->session), overhead, data_per_packet);
        return PLCTAG_ERR_TOO_LARGE;
    }

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);

    if(rc != PLCTAG_STATUS_OK) {
        tag->write_in_progress = 0;
        pdebug(DEBUG_WARN,"Unable to get new request.  rc=%d",rc);
        return rc;
    }

    /* point the struct pointers to the buffer*/
    lgx_pccc = (eip_cip_uc_req *)(req->data);
    embed_pccc = (embedded_pccc *)(lgx_pccc + 1);

    /* set up the embedded PCCC packet */
    embed_start = (uint8_t *)(embed_pccc);

    /* Command Routing */
    embed_pccc->service_code = AB_EIP_CMD_PCCC_EXECUTE;  /* ALWAYS 0x4B, Execute PCCC */
    embed_pccc->req_path_size = 2;   /* ALWAYS 2, size in words of path, next field */
    embed_pccc->req_path[0] = 0x20;  /* class */
    embed_pccc->req_path[1] = 0x67;  /* PCCC Execute */
    embed_pccc->req_path[2] = 0x24;  /* instance */
    embed_pccc->req_path[3] = 0x01;  /* instance 1 */

    /* Vendor info */
    embed_pccc->request_id_size = 7;  /* ALWAYS 7 */
    embed_pccc->vendor_id = h2le16(AB_EIP_VENDOR_ID);             /* Our CIP Vendor */
    embed_pccc->vendor_serial_number = h2le32(AB_EIP_VENDOR_SN);      /* our unique serial number */

    /* fill in the PCCC command */
    embed_pccc->pccc_command = AB_EIP_PCCC_TYPED_CMD;
    embed_pccc->pccc_status = 0;  /* STS 0 in request */
    embed_pccc->pccc_seq_num = h2le16(conn_seq_id); /* TODO - get sequence ID from session? */
    embed_pccc->pccc_function = AB_EIP_PCCCLGX_TYPED_WRITE_FUNC;
    embed_pccc->pccc_offset = h2le16(0);
    embed_pccc->pccc_transfer_size = h2le16((uint16_t)tag->elem_count); /* This is the offset items */

    /* point to the end of the struct */
    data = (uint8_t *)(embed_pccc + 1);

    /* copy encoded name  into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* copy the type info from the read. */
    mem_copy(data, tag->encoded_type_info, tag->encoded_type_info_size);
    data += tag->encoded_type_info_size;

    /* now copy the data to write */
    mem_copy(data,tag->data,tag->size);
    data += tag->size;

    /* if this is not an multiple of 16-bit chunks, pad it out */
    if((data - embed_start) & 0x01) {
        *data = 0;
        data++;
    }

    /*
     * after the embedded packet, we need to tell the message router
     * how to get to the target device.
     */

    /* encap fields */
    lgx_pccc->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND);    /* set up for unconnected sending */

    /* router timeout */
    lgx_pccc->router_timeout = h2le16(1);                 /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    lgx_pccc->cpf_item_count        = h2le16(2);                /* ALWAYS 2 */
    lgx_pccc->cpf_nai_item_type     = h2le16(AB_EIP_ITEM_NAI);  /* ALWAYS 0 */
    lgx_pccc->cpf_nai_item_length   = h2le16(0);                /* ALWAYS 0 */
    lgx_pccc->cpf_udi_item_type     = h2le16(AB_EIP_ITEM_UDI);  /* ALWAYS 0x00B2 - Unconnected Data Item */

    lgx_pccc->cm_service_code       = AB_EIP_CMD_UNCONNECTED_SEND;     /* unconnected send */
    lgx_pccc->cm_req_path_size      = 0x02;     /* path size in words */
    lgx_pccc->cm_req_path[0]        = 0x20;     /* class */
    lgx_pccc->cm_req_path[1]        = 0x06;     /* Connection Manager */
    lgx_pccc->cm_req_path[2]        = 0x24;     /* instance */
    lgx_pccc->cm_req_path[3]        = 0x01;     /* instance 1 */

    /* Unconnected send needs timeout information */
    lgx_pccc->secs_per_tick = AB_EIP_SECS_PER_TICK; /* seconds per tick */
    lgx_pccc->timeout_ticks = AB_EIP_TIMEOUT_TICKS; /* timeout = src_secs_per_tick * src_timeout_ticks */

    /* how big is the embedded packet? */
    lgx_pccc->uc_cmd_length = h2le16((uint16_t)(data - embed_start));

    /* copy the path */
    if(tag->session->conn_path_size > 0) {
        *data = (tag->session->conn_path_size) / 2; /* in 16-bit words */
        data++;
        *data = 0; /* reserved/pad */
        data++;
        mem_copy(data, tag->session->conn_path, tag->session->conn_path_size);
        data += tag->session->conn_path_size;
    }

    /* how big is the unconnected data item? */
    lgx_pccc->cpf_udi_item_length   = h2le16((uint16_t)(data - (uint8_t *)(&lgx_pccc->cm_service_code)));

    /* get ready to add the request to the queue for this session */
    req->request_size = (int)(data - (req->data));

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->write_in_progress = 0;
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}



/*
 * check_write_status
 *
 * Fragments are not supported.
 */
static int check_write_status(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    ab_request_p req;

    pdebug(DEBUG_SPEW,"Starting.");

    /* is there an outstanding request? */
    if (!tag->req) {
        tag->write_in_progress = 0;
        tag->offset = 0;

        pdebug(DEBUG_WARN,"Write in progress, but no request in flight!");

        return PLCTAG_ERR_WRITE;
    }

    /* request can be used by two threads at once. */
    spin_block(&tag->req->lock) {
        if(!tag->req->resp_received) {
            rc = PLCTAG_STATUS_PENDING;
            break;
        }

        /* check to see if it was an abort on the session side. */
        if(tag->req->status != PLCTAG_STATUS_OK) {
            rc = tag->req->status;
            tag->req->abort_request = 1;

            pdebug(DEBUG_WARN,"Session reported failure of request: %s.", plc_tag_decode_error(rc));

            tag->write_in_progress = 0;
            tag->offset = 0;

            break;
        }
    }

    if(rc != PLCTAG_STATUS_OK) {
        if(rc_is_error(rc)) {
            /* the request is dead, from session side. */
            tag->req = (ab_request_p)rc_dec(tag->req);
        }

        return rc;
    }

    /* the request is ours exclusively. */

    req = tag->req;

    /* fake exception */
    do {
        pccc_resp *pccc;
//        uint8_t *data;

        pccc = (pccc_resp *)(req->data);

        /* point to the start of the data */
//        data = (uint8_t *)pccc + sizeof(*pccc);

        /* check the response status */
        if( le2h16(pccc->encap_command) != AB_EIP_UNCONNECTED_SEND) {
            pdebug(DEBUG_WARN,"EIP unexpected response packet type: %d!",pccc->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if(le2h32(pccc->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN,"EIP command failed, response code: %d",le2h32(pccc->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc->general_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN,"PCCC command failed, response code: %d",pccc->general_status);
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc->pccc_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "PCCC command failed, response code: %d - %s", pccc->pccc_status, pccc_decode_error(&pccc->pccc_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request */
    tag->req = (ab_request_p)rc_dec(req);

    tag->write_in_progress = 0;

    pdebug(DEBUG_SPEW,"Done.");

    /* Success! */
    return rc;
}

#endif // __PROTOCOLS_AB_EIP_LGX_PCCC_C__


#ifndef __PROTOCOLS_AB_EIP_PLC5_DHP_C__
#define __PROTOCOLS_AB_EIP_PLC5_DHP_C__

static int check_read_status(ab_tag_p tag);
static int check_write_status(ab_tag_p tag);



static int tag_read_start(ab_tag_p tag);
static int tag_status(ab_tag_p tag);
static int tag_tickler(ab_tag_p tag);
static int tag_write_start(ab_tag_p tag);

struct tag_vtable_t eip_plc5_dhp_vtable = {
    (tag_vtable_func)ab_tag_abort, /* shared */
    (tag_vtable_func)tag_read_start,
    (tag_vtable_func)tag_status,
    (tag_vtable_func)tag_tickler,
    (tag_vtable_func)tag_write_start,
    (tag_vtable_func)NULL, /* wake_plc */

    /* data accessors */
    ab_get_int_attrib,
    ab_set_int_attrib
};


START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;    /* ALWAYS 0x0070 Connected Send */
    uint16_le encap_length;   /* packet size in bytes less the header size, which is 24 bytes */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;  /* whatever we want to set this to, used for
                                     * identifying responses when more than one
                                     * are in flight at once.
                                     */
    uint32_le options;               /* 0, reserved for future use */

    /* Interface Handle etc. */
    uint32_le interface_handle;      /* ALWAYS 0 */
    uint16_le router_timeout;        /* in seconds, zero for Connected Sends! */

    /* Common Packet Format - CPF Connected */
    uint16_le cpf_item_count;        /* ALWAYS 2 */
    uint16_le cpf_cai_item_type;     /* ALWAYS 0x00A1 Connected Address Item */
    uint16_le cpf_cai_item_length;   /* ALWAYS 2 ? */
    uint32_le cpf_targ_conn_id;           /* the connection id from Forward Open */
    uint16_le cpf_cdi_item_type;     /* ALWAYS 0x00B1, Connected Data Item type */
    uint16_le cpf_cdi_item_length;   /* length in bytes of the rest of the packet */

    /* Connection sequence number */
    uint16_le cpf_conn_seq_num;      /* connection sequence ID, inc for each message */

    /* PLC5 DH+ Routing */
    uint16_le dest_link;
    uint16_le dest_node;
    uint16_le src_link;
    uint16_le src_node;

    /* PCCC Command */
    uint8_t pccc_command;           /* CMD read, write etc. */
    uint8_t pccc_status;            /* STS 0x00 in request */
    uint16_le pccc_seq_num;          /* TNSW transaction/sequence id */
    uint8_t pccc_function;          /* FNC sub-function of command */
    //uint16_le pccc_transfer_offset;           /* offset of this request? */
    //uint16_le pccc_transfer_size;    /* number of elements requested */
} END_PACK pccc_dhp_co_req;


START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;    /* ALWAYS 0x0070 Connected Send */
    uint16_le encap_length;   /* packet size in bytes less the header size, which is 24 bytes */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;  /* whatever we want to set this to, used for
                                     * identifying responses when more than one
                                     * are in flight at once.
                                     */
    uint32_le options;               /* 0, reserved for future use */

    /* Interface Handle etc. */
    uint32_le interface_handle;      /* ALWAYS 0 */
    uint16_le router_timeout;        /* in seconds, zero for Connected Sends! */

    /* Common Packet Format - CPF Connected */
    uint16_le cpf_item_count;        /* ALWAYS 2 */
    uint16_le cpf_cai_item_type;     /* ALWAYS 0x00A1 Connected Address Item */
    uint16_le cpf_cai_item_length;   /* ALWAYS 2 ? */
    uint32_le cpf_targ_conn_id;           /* the connection id from Forward Open */
    uint16_le cpf_cdi_item_type;     /* ALWAYS 0x00B1, Connected Data Item type */
    uint16_le cpf_cdi_item_length;   /* length in bytes of the rest of the packet */

    /* connection ID from request */
    uint16_le cpf_conn_seq_num;      /* connection sequence ID, inc for each message */

    /* PLC5 DH+ Routing */
    uint16_le dest_link;
    uint16_le dest_node;
    uint16_le src_link;
    uint16_le src_node;

    /* PCCC Command */
    uint8_t pccc_command;           /* CMD read, write etc. */
    uint8_t pccc_status;            /* STS 0x00 in request */
    uint16_le pccc_seq_num;         /* TNSW transaction/connection sequence number */
    //uint8_t pccc_data[ZLA_SIZE];    /* data for PCCC request. */
} END_PACK pccc_dhp_co_resp;


/*
 * tag_status
 *
 * PCCC/DH+-specific status.  This functions as a "tickler" routine
 * to check on the completion of async requests.
 */
int tag_status(ab_tag_p tag)
{
    if (!tag->session) {
        /* this is not OK.  This is fatal! */
        return PLCTAG_ERR_CREATE;
    }

    if(tag->read_in_progress) {
        return PLCTAG_STATUS_PENDING;
    }

    if(tag->write_in_progress) {
        return PLCTAG_STATUS_PENDING;
    }

    return tag->status;
}




int tag_tickler(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW, "Starting.");

    if(tag->read_in_progress) {
        pdebug(DEBUG_SPEW, "Read in progress.");
        rc = check_read_status(tag);
        tag->status = (int8_t)rc;

        /* check to see if the read finished. */
        if(!tag->read_in_progress) {
            /* read done so create done. */
            if(tag->first_read) {
                tag->first_read = 0;
                tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_CREATED, (int8_t)rc);
            }

            tag->read_complete = 1;
        }

        return rc;
    }

    if(tag->write_in_progress) {
        pdebug(DEBUG_SPEW, "Write in progress.");
        rc = check_write_status(tag);
        tag->status = (int8_t)rc;

        /* check to see if the write finished. */
        if(!tag->write_in_progress) {
            tag->write_complete = 1;
        }

        return rc;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return tag->status;
}


/*
 * tag_read_start
 *
 * This does not support multiple request fragments.
 */
int tag_read_start(ab_tag_p tag)
{
    pccc_dhp_co_req *pccc;
    uint8_t *data = NULL;
    uint8_t *embed_start = NULL;
    int data_per_packet = 0;
    int overhead = 0;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p req = NULL;
    uint16_le transfer_offset = h2le16((uint16_t)0);
    uint16_le transfer_size = h2le16((uint16_t)0);

    pdebug(DEBUG_INFO, "Starting");

    if(tag->read_in_progress || tag->write_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    tag->read_in_progress = 1;

    /* What is the overhead in the _response_ */
    overhead =   1  /* pccc command */
                 +1  /* pccc status */
                 +2;  /* pccc sequence num */

    data_per_packet = session_get_max_payload(tag->session) - overhead;

    if(data_per_packet <= 0) {
        tag->read_in_progress = 0;
        pdebug(DEBUG_WARN, "Unable to send request.  Packet overhead, %d bytes, is too large for packet, %d bytes!", overhead, session_get_max_payload(tag->session));
        return PLCTAG_ERR_TOO_LARGE;
    }

    if(data_per_packet < tag->size) {
        tag->read_in_progress = 0;
        pdebug(DEBUG_DETAIL, "Unable to send request: Tag size is %d, write overhead is %d, and write data per packet is %d!", tag->size, overhead, data_per_packet);
        return PLCTAG_ERR_TOO_LARGE;
    }

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if(rc != PLCTAG_STATUS_OK) {
        tag->read_in_progress = 0;
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    pccc = (pccc_dhp_co_req *)(req->data);

    /* point to the end of the struct */
    data = (req->data) + sizeof(*pccc);

    embed_start = (uint8_t *)(&(pccc->cpf_conn_seq_num));

    /* this kind of PCCC function takes an offset and size. */
    transfer_offset = h2le16((uint16_t)0);
    mem_copy(data, &transfer_offset, (int)(unsigned int)sizeof(transfer_offset));
    data += sizeof(transfer_offset);

    transfer_size = h2le16((uint16_t)((tag->size)/2));
    mem_copy(data, &transfer_size, (int)(unsigned int)sizeof(transfer_size));
    data += sizeof(transfer_size);

    /* copy encoded tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* amount of data to get this time */
    *data = (uint8_t)(tag->size); /* bytes for this transfer */
    data++;

    /* encap fields */
    pccc->encap_command = h2le16(AB_EIP_CONNECTED_SEND);    /* ALWAYS 0x006F Unconnected Send*/

    /* router timeout */
    pccc->router_timeout = h2le16(1);                 /* one second timeout, enough? */

    /* Common Packet Format fields */
    pccc->cpf_item_count = h2le16(2);                 /* ALWAYS 2 */
    pccc->cpf_cai_item_type = h2le16(AB_EIP_ITEM_CAI);/* ALWAYS 0x00A1 connected address item */
    pccc->cpf_cai_item_length = h2le16(4);            /* ALWAYS 4 ? */
    pccc->cpf_cdi_item_type = h2le16(AB_EIP_ITEM_CDI);/* ALWAYS 0x00B1 - connected Data Item */
    pccc->cpf_cdi_item_length = h2le16((uint16_t)(data - embed_start)); /* REQ: fill in with length of remaining data. */

    /* DH+ Routing */
    pccc->dest_link = h2le16(0);
    pccc->dest_node = h2le16(tag->session->dhp_dest);
    pccc->src_link = h2le16(0);
    pccc->src_node = h2le16(0) /*h2le16(tag->dhp_src)*/;

    /* PCCC Command */
    pccc->pccc_command = AB_EIP_PCCC_TYPED_CMD;
    pccc->pccc_status = 0;  /* STS 0 in request */
    pccc->pccc_seq_num = /*h2le16(conn_seq_id)*/ h2le16((uint16_t)(intptr_t)(tag->session));
    pccc->pccc_function = AB_EIP_PLC5_RANGE_READ_FUNC;
    //pccc->pccc_transfer_offset = h2le16((uint16_t)0);
    //pccc->pccc_transfer_size = h2le16((uint16_t)((tag->size)/2));  /* size in 2-byte words */

    pdebug(DEBUG_DETAIL, "Total data length %d.", (int)(unsigned int)(data - (uint8_t*)(pccc)));
    pdebug(DEBUG_DETAIL, "Total payload length %d.", (int)(unsigned int)(data - embed_start));

    /* get ready to add the request to the queue for this session */
    req->request_size = (int)(data - (req->data));

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if(rc != PLCTAG_STATUS_OK) {
        tag->read_in_progress = 0;
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        req->abort_request = 1;
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;
//    tag->status = PLCTAG_STATUS_PENDING;

    /* the read is now pending */
    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}



int tag_write_start(ab_tag_p tag)
{
    pccc_dhp_co_req *pccc;
    uint8_t *data = NULL;
    uint8_t *embed_start = NULL;
    int data_per_packet = 0;
    int overhead = 0;
    uint16_t conn_seq_id = (uint16_t)(session_get_new_seq_id(tag->session));;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p req;
    uint16_le transfer_offset = h2le16((uint16_t)0);
    uint16_le transfer_size = h2le16((uint16_t)0);

    pdebug(DEBUG_INFO, "Starting");

    if(tag->read_in_progress || tag->write_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    tag->write_in_progress = 1;

    /* how many packets will we need? How much overhead? */
    overhead = 2        /* size of sequence num */
               +8        /* DH+ routing */
               +1        /* DF1 command */
               +1        /* status */
               +2        /* PCCC packet sequence number */
               +1        /* PCCC function */
               +2        /* request offset */
               +2        /* tag size in elements */
               +(tag->encoded_name_size)
               +2;       /* this request size in elements */

    data_per_packet = session_get_max_payload(tag->session) - overhead;

    if(data_per_packet <= 0) {
        tag->write_in_progress = 0;
        pdebug(DEBUG_WARN, "Unable to send request.  Packet overhead, %d bytes, is too large for packet, %d bytes!", overhead, session_get_max_payload(tag->session));
        return PLCTAG_ERR_TOO_LARGE;
    }

    if(data_per_packet < tag->size) {
        tag->write_in_progress = 0;
        pdebug(DEBUG_WARN, "PCCC requests cannot be fragmented.  Too much data requested.");
        return PLCTAG_ERR_TOO_LARGE;
    }

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);

    if(rc != PLCTAG_STATUS_OK) {
        tag->write_in_progress = 0;
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    pccc = (pccc_dhp_co_req *)(req->data);

    embed_start = (uint8_t *)(&pccc->cpf_conn_seq_num);

    /* point to the end of the struct */
    data = (req->data) + sizeof(*pccc);

    /* this kind of PCCC function takes an offset and size.  Only if not a bit tag. */
    if(!tag->is_bit) {
        transfer_offset = h2le16((uint16_t)0);
        mem_copy(data, &transfer_offset, (int)(unsigned int)sizeof(transfer_offset));
        data += sizeof(transfer_offset);

        transfer_size = h2le16((uint16_t)((tag->size)/2));
        mem_copy(data, &transfer_size, (int)(unsigned int)sizeof(transfer_size));
        data += sizeof(transfer_size);
    }

    /* copy encoded tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* now copy the data to write */
    if(!tag->is_bit) {
        mem_copy(data, tag->data, tag->size);
        data += tag->size;
    } else {
        /* AND/reset mask */
        for(int i=0; i < tag->elem_size; i++) {
            if((tag->bit / 8) == i) {
                /* only reset if the tag data bit is not set */
                uint8_t mask = (uint8_t)(1 << (tag->bit % 8));

                /* _unset_ if the bit is not set. */
                if(tag->data[i] & mask) {
                    *data = (uint8_t)0xFF;
                } else {
                    *data = (uint8_t)~mask;
                }

                pdebug(DEBUG_DETAIL, "adding reset mask byte %d: %x", i, *data);

                data++;
            } else {
                /* this is not the data we care about. */
                *data = (uint8_t)0xFF;

                pdebug(DEBUG_DETAIL, "adding reset mask byte %d: %x", i, *data);

                data++;
            }
        }

        /* OR/set mask */
        for(int i=0; i < tag->elem_size; i++) {
            if((tag->bit / 8) == i) {
                /* only set if the tag data bit is set */
                *data = tag->data[i] & (uint8_t)(1 << (tag->bit % 8));

                pdebug(DEBUG_DETAIL, "adding set mask byte %d: %x", i, *data);

                data++;
            } else {
                /* this is not the data we care about. */
                *data = (uint8_t)0x00;

                pdebug(DEBUG_DETAIL, "adding set mask byte %d: %x", i, *data);

                data++;
            }
        }
    }

    /* now fill in the rest of the structure. */

    /* encap fields */
    pccc->encap_command = h2le16(AB_EIP_CONNECTED_SEND);    /* ALWAYS 0x006F Unconnected Send*/

    /* router timeout */
    pccc->router_timeout = h2le16(1);                 /* one second timeout, enough? */

    /* Common Packet Format fields */
    pccc->cpf_item_count = h2le16(2);                 /* ALWAYS 2 */
    pccc->cpf_cai_item_type = h2le16(AB_EIP_ITEM_CAI);/* ALWAYS 0x00A1 connected address item */
    pccc->cpf_cai_item_length = h2le16(4);            /* ALWAYS 4 ? */
    pccc->cpf_cdi_item_type = h2le16(AB_EIP_ITEM_CDI);/* ALWAYS 0x00B1 - connected Data Item */
    pccc->cpf_cdi_item_length = h2le16((uint16_t)(data - embed_start)); /* REQ: fill in with length of remaining data. */

    pdebug(DEBUG_DETAIL, "Total data length %d.", (int)(unsigned int)(data - (uint8_t*)(pccc)));
    pdebug(DEBUG_DETAIL, "Total payload length %d.", (int)(unsigned int)(data - embed_start));

    /* DH+ Routing */
    pccc->dest_link = h2le16(0);
    pccc->dest_node = h2le16(tag->session->dhp_dest);
    pccc->src_link = h2le16(0);
    pccc->src_node = h2le16(0) /*h2le16(tag->dhp_src)*/;

    /* PCCC Command */
    pccc->pccc_command = AB_EIP_PCCC_TYPED_CMD;
    pccc->pccc_status = 0;  /* STS 0 in request */
    pccc->pccc_seq_num = h2le16(conn_seq_id); /* FIXME - get sequence ID from session? */
    pccc->pccc_function = (tag->is_bit ? AB_EIP_PLC5_RMW_FUNC : AB_EIP_PLC5_RANGE_WRITE_FUNC);
    //pccc->pccc_transfer_offset = h2le16((uint16_t)0);
    //pccc->pccc_transfer_size = h2le16((uint16_t)((tag->size)/2));  /* size in 2-byte words */

    /* get ready to add the request to the queue for this session */
    req->request_size = (int)(data - (req->data));

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if(rc != PLCTAG_STATUS_OK) {
        tag->write_in_progress = 0;
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        req->abort_request = 1;
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;
//    tag->status = PLCTAG_STATUS_PENDING;

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}


/*
 * check_read_status
 *
 * PCCC does not support request fragments.
 */
static int check_read_status(ab_tag_p tag)
{
    pccc_dhp_co_resp *resp;
    uint8_t *data;
    uint8_t *data_end;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_read_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Read request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    resp = (pccc_dhp_co_resp *)(request->data);

    /* point to the start of the data */
    data = (uint8_t *)resp + sizeof(*resp);

    /* point to the end of the data */
    data_end = (request->data + le2h16(resp->encap_length) + sizeof(eip_encap));

    /* fake exception */
    do {
        if(le2h16(resp->encap_command) != AB_EIP_CONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if(le2h32(resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(resp->pccc_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "PCCC command failed, response code: %d - %s", resp->pccc_status, pccc_decode_error(&resp->pccc_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        /* did we get the right amount of data? */
        if((data_end - data) != tag->size) {
            if((int)(data_end - data) > tag->size) {
                pdebug(DEBUG_WARN, "Too much data received!  Expected %d bytes but got %d bytes!", tag->size, (int)(data_end - data));
                rc = PLCTAG_ERR_TOO_LARGE;
            } else {
                pdebug(DEBUG_WARN, "Too little data received!  Expected %d bytes but got %d bytes!", tag->size, (int)(data_end - data));
                rc = PLCTAG_ERR_TOO_SMALL;
            }
            break;
        }

        /* copy data into the tag. */
        mem_copy(tag->data, data, (int)(data_end - data));

        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request. */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    tag->read_in_progress = 0;

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}


static int check_write_status(ab_tag_p tag)
{
    pccc_dhp_co_resp *pccc_resp;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_write_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Write request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    pccc_resp = (pccc_dhp_co_resp *)(request->data);

    /* fake exception */
    do {
        /* check the response status */
        if( le2h16(pccc_resp->encap_command) != AB_EIP_CONNECTED_SEND) {
            pdebug(DEBUG_WARN, "EIP unexpected response packet type: %d!", pccc_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if(le2h32(pccc_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(pccc_resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc_resp->pccc_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "PCCC command failed, response code: %d - %s", pccc_resp->pccc_status, pccc_decode_error(&pccc_resp->pccc_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        /* everything OK */
        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request. */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);
    
    tag->write_in_progress = 0;

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}

#endif // __PROTOCOLS_AB_EIP_PLC5_DHP_C__


#ifndef __PROTOCOLS_AB_EIP_PLC5_PCCC_C__
#define __PROTOCOLS_AB_EIP_PLC5_PCCC_C__

/* PCCC */
static int tag_read_start(ab_tag_p tag);
static int tag_status(ab_tag_p tag);
static int tag_tickler(ab_tag_p tag);
static int tag_write_start(ab_tag_p tag);

struct tag_vtable_t plc5_vtable = {
    (tag_vtable_func)ab_tag_abort, /* shared */
    (tag_vtable_func)tag_read_start,
    (tag_vtable_func)tag_status,
    (tag_vtable_func)tag_tickler,
    (tag_vtable_func)tag_write_start,
    (tag_vtable_func)NULL, /* wake_plc */

    /* data accessors */
    ab_get_int_attrib,
    ab_set_int_attrib
};


/* default string types used for PLC-5 PLCs. */
tag_byte_order_t plc5_tag_byte_order = {
    .is_allocated = 0,

    .str_is_defined = 1,
    .str_is_counted = 1,
    .str_is_fixed_length = 1,
    .str_is_zero_terminated = 0,
    .str_is_byte_swapped = 1,

    .str_count_word_bytes = 2,
    .str_max_capacity = 82,
    .str_total_length = 84,
    .str_pad_bytes = 0,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {2,3,0,1}, /* yes, it is that weird. */
    .float64_order = {0,1,2,3,4,5,6,7},
};


static int check_read_status(ab_tag_p tag);
static int check_write_status(ab_tag_p tag);

START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;         /* ALWAYS 0x006f Unconnected Send*/
    uint16_le encap_length;          /* packet size in bytes - 24 */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;  /* whatever we want to set this to, used for
                                     * identifying responses when more than one
                                     * are in flight at once.
                                     */
    uint32_le encap_options;         /* 0, reserved for future use */

    /* Interface Handle etc. */
    uint32_le interface_handle;      /* ALWAYS 0 */
    uint16_le router_timeout;        /* in seconds, 5 or 10 seems to be good.*/

    /* Common Packet Format - CPF Unconnected */
    uint16_le cpf_item_count;        /* ALWAYS 2 */
    uint16_le cpf_nai_item_type;     /* ALWAYS 0 */
    uint16_le cpf_nai_item_length;   /* ALWAYS 0 */
    uint16_le cpf_udi_item_type;     /* ALWAYS 0x00B2 - Unconnected Data Item */
    uint16_le cpf_udi_item_length;   /* REQ: fill in with length of remaining data. */

    /* PCCC Command Req Routing */
    uint8_t service_code;           /* ALWAYS 0x4B, Execute PCCC */
    uint8_t req_path_size;          /* ALWAYS 0x02, in 16-bit words */
    uint8_t req_path[4];            /* ALWAYS 0x20,0x67,0x24,0x01 for PCCC */
    uint8_t request_id_size;        /* ALWAYS 7 */
    uint16_le vendor_id;             /* Our CIP Vendor ID */
    uint32_le vendor_serial_number;  /* Our CIP Vendor Serial Number */

    /* PCCC Command */
    uint8_t pccc_command;            /* CMD read, write etc. */
    uint8_t pccc_status;             /* STS 0x00 in request */
    uint16_le pccc_seq_num;          /* TNS transaction/sequence id */
    uint8_t pccc_function;           /* FNC sub-function of command */
    //uint16_le pccc_transfer_offset;  /* offset of requested in total request */
    //uint16_le pccc_transfer_size;    /* total number of elements requested */
} END_PACK pccc_req;


/*
 * tag_status
 *
 * get the tag status.
 */
int tag_status(ab_tag_p tag)
{
    if (!tag->session) {
        /* this is not OK.  This is fatal! */
        return PLCTAG_ERR_CREATE;
    }

    if(tag->read_in_progress) {
        return PLCTAG_STATUS_PENDING;
    }

    if(tag->write_in_progress) {
        return PLCTAG_STATUS_PENDING;
    }

    return tag->status;
}




int tag_tickler(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW, "Starting.");

    if(tag->read_in_progress) {
        pdebug(DEBUG_SPEW, "Read in progress.");
        rc = check_read_status(tag);
        tag->status = (int8_t)rc;

        /* check to see if the read finished. */
        if(!tag->read_in_progress) {
            /* first read done */
            if(tag->first_read) {
                tag->first_read = 0;
                tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_CREATED, PLCTAG_STATUS_OK);
            }

            tag->read_complete = 1;
        }

        return rc;
    }

    if(tag->write_in_progress) {
        pdebug(DEBUG_SPEW, "Write in progress.");
        rc = check_write_status(tag);
        tag->status = (int8_t)rc;

        /* check to see if the write finished. */
        if(!tag->write_in_progress) {
            tag->write_complete = 1;
        }

        return rc;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return tag->status;

}


/*
 * tag_read_start
 *
 * Start a PCCC tag read (PLC5).
 */

int tag_read_start(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    ab_request_p req = NULL;
    uint16_t conn_seq_id = (uint16_t)(session_get_new_seq_id(tag->session));;
    int overhead;
    int data_per_packet;
    pccc_req *pccc;
    uint8_t *data;
    uint8_t *embed_start;
    uint16_le transfer_offset = h2le16(0);
    uint16_le transfer_size = h2le16(0);

    pdebug(DEBUG_INFO, "Starting");

    if(tag->read_in_progress || tag->write_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    tag->read_in_progress = 1;

    /* What is the overhead in the _response_ */
    overhead =   1  /* pccc command */
                 +1  /* pccc status */
                 +2;  /* pccc sequence num */

    data_per_packet = session_get_max_payload(tag->session) - overhead;

    if(data_per_packet <= 0) {
        pdebug(DEBUG_WARN, "Unable to send request.  Packet overhead, %d bytes, is too large for packet, %d bytes!", overhead, session_get_max_payload(tag->session));
        tag->read_in_progress = 0;
        return PLCTAG_ERR_TOO_LARGE;
    }

    if(data_per_packet < tag->size) {
        pdebug(DEBUG_DETAIL, "Unable to send request: Tag size is %d, write overhead is %d, and write data per packet is %d!", tag->size, overhead, data_per_packet);
        tag->read_in_progress = 0;
        return PLCTAG_ERR_TOO_LARGE;
    }

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to get new request.  rc=%d", rc);
        tag->read_in_progress = 0;
        return rc;
    }

    /* point the struct pointers to the buffer*/
    pccc = (pccc_req *)(req->data);

    /* set up the embedded PCCC packet */
    embed_start = (uint8_t *)(&pccc->service_code);

    /* Command Routing */
    pccc->service_code = AB_EIP_CMD_PCCC_EXECUTE;  /* ALWAYS 0x4B, Execute PCCC */
    pccc->req_path_size = 2;   /* ALWAYS 2, size in words of path, next field */
    pccc->req_path[0] = 0x20;  /* class */
    pccc->req_path[1] = 0x67;  /* PCCC Execute */
    pccc->req_path[2] = 0x24;  /* instance */
    pccc->req_path[3] = 0x01;  /* instance 1 */

    /* PCCC ID */
    pccc->request_id_size = 7;  /* ALWAYS 7 */
    pccc->vendor_id = h2le16(AB_EIP_VENDOR_ID);             /* Our CIP Vendor */
    pccc->vendor_serial_number = h2le32(AB_EIP_VENDOR_SN);      /* our unique serial number */

    /* fill in the PCCC command */
    pccc->pccc_command = AB_EIP_PCCC_TYPED_CMD;
    pccc->pccc_status = 0;  /* STS 0 in request */
    pccc->pccc_seq_num = h2le16(conn_seq_id); /* FIXME - get sequence ID from session? */
    pccc->pccc_function = AB_EIP_PLC5_RANGE_READ_FUNC;
    //pccc->pccc_transfer_offset = h2le16((uint16_t)0);
    //pccc->pccc_transfer_size = h2le16((uint16_t)((tag->size)/2));  /* size in 2-byte words */

    /* point to the end of the struct */
    data = ((uint8_t *)pccc) + sizeof(pccc_req);

    /* this kind of PCCC function takes an offset and size. */
    transfer_offset = h2le16((uint16_t)0);
    mem_copy(data, &transfer_offset, (int)(unsigned int)sizeof(transfer_offset));
    data += sizeof(transfer_offset);

    transfer_size = h2le16((uint16_t)((tag->size)/2));
    mem_copy(data, &transfer_size, (int)(unsigned int)sizeof(transfer_size));
    data += sizeof(transfer_size);

    /* copy encoded tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* amount of data to get this time */
    *data = (uint8_t)(tag->size); /* bytes for this transfer */
    data++;

    /*
     * after the embedded packet, we need to tell the message router
     * how to get to the target device.
     */

    /* encap fields */
    pccc->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND);    /* set up for unconnected sending */

    /* router timeout */
    pccc->router_timeout = h2le16(1);                 /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    pccc->cpf_item_count        = h2le16(2);                /* ALWAYS 2 */
    pccc->cpf_nai_item_type     = h2le16(AB_EIP_ITEM_NAI);  /* ALWAYS 0 */
    pccc->cpf_nai_item_length   = h2le16(0);                /* ALWAYS 0 */
    pccc->cpf_udi_item_type     = h2le16(AB_EIP_ITEM_UDI);  /* ALWAYS 0x00B2 - Unconnected Data Item */
    pccc->cpf_udi_item_length   = h2le16((uint16_t)(data - embed_start));  /* REQ: fill in with length of remaining data. */

    /* set the size of the request */
    req->request_size = (int)(data - (req->data));

    /* mark it as ready to send */
    //req->send_request = 1;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        req->abort_request = 1;
        tag->read_in_progress = 0;

        tag->req = (ab_request_p)rc_dec(req);

        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}





/*
 * check_read_status
 *
 * NOTE that we can have only one outstanding request because PCCC
 * does not support fragments.
 */


static int check_read_status(ab_tag_p tag)
{
    pccc_resp *pccc;
    uint8_t *data;
    uint8_t *data_end;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_read_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Read request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    pccc = (pccc_resp *)(request->data);

    /* point to the start of the data */
    data = (uint8_t *)pccc + sizeof(*pccc);

    /* point to the end of the data */
    data_end = (request->data + le2h16(pccc->encap_length) + sizeof(eip_encap));

    /* fake exceptions */
    do {
        if(le2h16(pccc->encap_command) != AB_EIP_UNCONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", pccc->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if(le2h32(pccc->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(pccc->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc->general_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "PCCC command failed, response code: (%d) %s", pccc->general_status, decode_cip_error_long((uint8_t *)&(pccc->general_status)));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc->pccc_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "PCCC command failed, response code: %d - %s", pccc->pccc_status, pccc_decode_error(&pccc->pccc_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        /* did we get the right amount of data? */
        if((data_end - data) != tag->size) {
            if((int)(data_end - data) > tag->size) {
                pdebug(DEBUG_WARN, "Too much data received!  Expected %d bytes but got %d bytes!", tag->size, (int)(data_end - data));
                rc = PLCTAG_ERR_TOO_LARGE;
            } else {
                pdebug(DEBUG_WARN, "Too little data received!  Expected %d bytes but got %d bytes!", tag->size, (int)(data_end - data));
                rc = PLCTAG_ERR_TOO_SMALL;
            }
            break;
        }

        /* copy data into the tag. */
        mem_copy(tag->data, data, (int)(data_end - data));

        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request. */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    tag->read_in_progress = 0;

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}





/* FIXME  convert to unconnected messages. */

int tag_write_start(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    pccc_req *pccc;
    uint8_t *data;
    uint16_t conn_seq_id = (uint16_t)(session_get_new_seq_id(tag->session));;
    uint8_t *embed_start;
    int overhead, data_per_packet;
    ab_request_p req = NULL;
    uint16_le transfer_offset = h2le16((uint16_t)0);
    uint16_le transfer_size = h2le16((uint16_t)0);

    pdebug(DEBUG_INFO, "Starting.");

    if(tag->read_in_progress || tag->write_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    tag->write_in_progress = 1;

    /* How much overhead? */
    overhead =   1  /* pccc command */
                 +1  /* pccc status */
                 +2  /* pccc sequence num */
                 +1  /* pccc function */
                 +2  /* transfer offset, in words? */
                 +2  /* total transfer size in words */
                 +tag->encoded_name_size
                 +1; /* size in bytes of this write */

    data_per_packet = session_get_max_payload(tag->session) - overhead;

    if(data_per_packet <= 0) {
        pdebug(DEBUG_WARN, "Unable to send request.  Packet overhead, %d bytes, is too large for packet, %d bytes!", overhead, session_get_max_payload(tag->session));
        tag->write_in_progress = 0;
        return PLCTAG_ERR_TOO_LARGE;
    }

    if(data_per_packet < tag->size) {
        pdebug(DEBUG_DETAIL, "Tag size is %d, write overhead is %d, and write data per packet is %d.", session_get_max_payload(tag->session), overhead, data_per_packet);
        tag->write_in_progress = 0;
        return PLCTAG_ERR_TOO_LARGE;
    }

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to get new request.  rc=%d", rc);
        tag->write_in_progress = 0;
        return rc;
    }

    pccc = (pccc_req *)(req->data);

    /* set up the embedded PCCC packet */
    embed_start = (uint8_t *)(&pccc->service_code);

    /* point to the end of the struct */
    data = (req->data) + sizeof(pccc_req);

    /* this kind of PCCC function takes an offset and size.  Only if not a bit tag. */
    if(!tag->is_bit) {
        transfer_offset = h2le16((uint16_t)0);
        mem_copy(data, &transfer_offset, (int)(unsigned int)sizeof(transfer_offset));
        data += sizeof(transfer_offset);

        transfer_size = h2le16((uint16_t)((tag->size)/2));
        mem_copy(data, &transfer_size, (int)(unsigned int)sizeof(transfer_size));
        data += sizeof(transfer_size);
    }

    /* copy encoded tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* now copy the data to write */
    if(!tag->is_bit) {
        mem_copy(data, tag->data, tag->size);
        data += tag->size;
    } else {
        /* AND/reset mask */
        for(int i=0; i < tag->elem_size; i++) {
            if((tag->bit / 8) == i) {
                /* only reset if the tag data bit is not set */
                uint8_t mask = (uint8_t)(1 << (tag->bit % 8));

                /* _unset_ if the bit is not set. */
                if(tag->data[i] & mask) {
                    *data = (uint8_t)0xFF;
                } else {
                    *data = (uint8_t)~mask;
                }

                pdebug(DEBUG_DETAIL, "adding reset mask byte %d: %x", i, *data);

                data++;
            } else {
                /* this is not the data we care about. */
                *data = (uint8_t)0xFF;

                pdebug(DEBUG_DETAIL, "adding reset mask byte %d: %x", i, *data);

                data++;
            }
        }

        /* OR/set mask */
        for(int i=0; i < tag->elem_size; i++) {
            if((tag->bit / 8) == i) {
                /* only set if the tag data bit is set */
                *data = tag->data[i] & (uint8_t)(1 << (tag->bit % 8));

                pdebug(DEBUG_DETAIL, "adding set mask byte %d: %x", i, *data);

                data++;
            } else {
                /* this is not the data we care about. */
                *data = (uint8_t)0x00;

                pdebug(DEBUG_DETAIL, "adding set mask byte %d: %x", i, *data);

                data++;
            }
        }
    }

    /* now fill in the rest of the structure. */

    /* encap fields */
    pccc->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND);

    /* router timeout */
    pccc->router_timeout = h2le16(1);                 /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    pccc->cpf_item_count        = h2le16(2);                /* ALWAYS 2 */
    pccc->cpf_nai_item_type     = h2le16(AB_EIP_ITEM_NAI);  /* ALWAYS 0 */
    pccc->cpf_nai_item_length   = h2le16(0);                /* ALWAYS 0 */
    pccc->cpf_udi_item_type     = h2le16(AB_EIP_ITEM_UDI);  /* ALWAYS 0x00B2 - Unconnected Data Item */
    pccc->cpf_udi_item_length   = h2le16((uint16_t)(data - embed_start));  /* REQ: fill in with length of remaining data. */

    /* Command Routing */
    pccc->service_code = AB_EIP_CMD_PCCC_EXECUTE;  /* ALWAYS 0x4B, Execute PCCC */
    pccc->req_path_size = 2;   /* ALWAYS 2, size in words of path, next field */
    pccc->req_path[0] = 0x20;  /* class */
    pccc->req_path[1] = 0x67;  /* PCCC Execute */
    pccc->req_path[2] = 0x24;  /* instance */
    pccc->req_path[3] = 0x01;  /* instance 1 */

    /* PCCC ID */
    pccc->request_id_size = 7;  /* ALWAYS 7 */
    pccc->vendor_id = h2le16(AB_EIP_VENDOR_ID);                 /* Our CIP Vendor */
    pccc->vendor_serial_number = h2le32(AB_EIP_VENDOR_SN);      /* our unique serial number */

    /* PCCC Command */
    pccc->pccc_command = AB_EIP_PCCC_TYPED_CMD;
    pccc->pccc_status = 0;  /* STS 0 in request */
    pccc->pccc_seq_num = h2le16(conn_seq_id); /* FIXME - get sequence ID from session? */
    pccc->pccc_function = (tag->is_bit ? AB_EIP_PLC5_RMW_FUNC : AB_EIP_PLC5_RANGE_WRITE_FUNC);
    //pccc->pccc_transfer_offset = h2le16((uint16_t)0);
    //pccc->pccc_transfer_size = h2le16((uint16_t)((tag->size)/2));  /* size in 2-byte words */

    /* get ready to add the request to the queue for this session */
    req->request_size = (int)(data - (req->data));

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        req->abort_request = 1;
        tag->write_in_progress = 0;

        tag->req = (ab_request_p)rc_dec(req);

        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}



/*
 * check_write_status
 *
 * Fragments are not supported.
 */
static int check_write_status(ab_tag_p tag)
{
    pccc_resp *pccc;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_write_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Write request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    pccc = (pccc_resp *)(request->data);

    /* fake exception */
    do {
        /* check the response status */
        if( le2h16(pccc->encap_command) != AB_EIP_UNCONNECTED_SEND) {
            pdebug(DEBUG_WARN, "EIP unexpected response packet type: %d!", pccc->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if(le2h32(pccc->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(pccc->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc->general_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "PCCC command failed, response code: %d", pccc->general_status);
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc->pccc_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "PCCC command failed, response code: %d - %s", pccc->pccc_status, pccc_decode_error(&pccc->pccc_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request. */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    tag->write_in_progress = 0;

    pdebug(DEBUG_SPEW, "Done.");

    /* Success! */
    return rc;
}

#endif // __PROTOCOLS_AB_EIP_PLC5_PCCC_C__


#ifndef __PROTOCOLS_AB_EIP_SLC_DHP_C__
#define __PROTOCOLS_AB_EIP_SLC_DHP_C__

static int check_read_status(ab_tag_p tag);
static int check_write_status(ab_tag_p tag);



static int tag_read_start(ab_tag_p tag);
static int tag_status(ab_tag_p tag);
static int tag_tickler(ab_tag_p tag);
static int tag_write_start(ab_tag_p tag);

struct tag_vtable_t eip_slc_dhp_vtable = {
    (tag_vtable_func)ab_tag_abort, /* shared */
    (tag_vtable_func)tag_read_start,
    (tag_vtable_func)tag_status,
    (tag_vtable_func)tag_tickler,
    (tag_vtable_func)tag_write_start,
    (tag_vtable_func)NULL, /* wake_plc */

    /* data accessors */
    ab_get_int_attrib,
    ab_set_int_attrib
};


START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;    /* ALWAYS 0x0070 Connected Send */
    uint16_le encap_length;   /* packet size in bytes less the header size, which is 24 bytes */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;  /* whatever we want to set this to, used for
                                     * identifying responses when more than one
                                     * are in flight at once.
                                     */
    uint32_le options;               /* 0, reserved for future use */

    /* Interface Handle etc. */
    uint32_le interface_handle;      /* ALWAYS 0 */
    uint16_le router_timeout;        /* in seconds, zero for Connected Sends! */

    /* Common Packet Format - CPF Connected */
    uint16_le cpf_item_count;        /* ALWAYS 2 */
    uint16_le cpf_cai_item_type;     /* ALWAYS 0x00A1 Connected Address Item */
    uint16_le cpf_cai_item_length;   /* ALWAYS 2 ? */
    uint32_le cpf_targ_conn_id;           /* the connection id from Forward Open */
    uint16_le cpf_cdi_item_type;     /* ALWAYS 0x00B1, Connected Data Item type */
    uint16_le cpf_cdi_item_length;   /* length in bytes of the rest of the packet */

    /* Connection sequence number */
    uint16_le cpf_conn_seq_num;      /* connection sequence ID, inc for each message */

    /* SLC DH+ Routing */
    uint16_le dest_link;
    uint16_le dest_node;
    uint16_le src_link;
    uint16_le src_node;

    // /* PCCC Command */
    // uint8_t pccc_command;           /* CMD read, write etc. */
    // uint8_t pccc_status;            /* STS 0x00 in request */
    // uint16_le pccc_seq_num;          /* TNSW transaction/sequence id */
    // uint8_t pccc_function;          /* FNC sub-function of command */
    // uint16_le pccc_transfer_offset;           /* offset of this request? */
    // uint16_le pccc_transfer_size;    /* number of elements requested */

    /* SLC/MicroLogix PCCC Command */
    uint8_t pccc_command;           /* CMD read, write etc. */
    uint8_t pccc_status;            /* STS 0x00 in request */
    uint16_le pccc_seq_num;         /* TNS transaction/sequence id */
    uint8_t pccc_function;          /* FNC sub-function of command */
    uint8_t pccc_transfer_size;     /* total number of bytes requested */

} END_PACK pccc_dhp_co_req;


START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;    /* ALWAYS 0x0070 Connected Send */
    uint16_le encap_length;   /* packet size in bytes less the header size, which is 24 bytes */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;  /* whatever we want to set this to, used for
                                     * identifying responses when more than one
                                     * are in flight at once.
                                     */
    uint32_le options;               /* 0, reserved for future use */

    /* Interface Handle etc. */
    uint32_le interface_handle;      /* ALWAYS 0 */
    uint16_le router_timeout;        /* in seconds, zero for Connected Sends! */

    /* Common Packet Format - CPF Connected */
    uint16_le cpf_item_count;        /* ALWAYS 2 */
    uint16_le cpf_cai_item_type;     /* ALWAYS 0x00A1 Connected Address Item */
    uint16_le cpf_cai_item_length;   /* ALWAYS 2 ? */
    uint32_le cpf_targ_conn_id;           /* the connection id from Forward Open */
    uint16_le cpf_cdi_item_type;     /* ALWAYS 0x00B1, Connected Data Item type */
    uint16_le cpf_cdi_item_length;   /* length in bytes of the rest of the packet */

    /* connection ID from request */
    uint16_le cpf_conn_seq_num;      /* connection sequence ID, inc for each message */

    /* PLC5 DH+ Routing */
    uint16_le dest_link;
    uint16_le dest_node;
    uint16_le src_link;
    uint16_le src_node;

    /* PCCC Command */
    uint8_t pccc_command;           /* CMD read, write etc. */
    uint8_t pccc_status;            /* STS 0x00 in request */
    uint16_le pccc_seq_num;         /* TNSW transaction/connection sequence number */
    //uint8_t pccc_data[ZLA_SIZE];    /* data for PCCC request. */
} END_PACK pccc_dhp_co_resp;


/*
 * tag_status
 *
 * PCCC/DH+-specific status.  This functions as a "tickler" routine
 * to check on the completion of async requests.
 */
int tag_status(ab_tag_p tag)
{
    if (!tag->session) {
        /* this is not OK.  This is fatal! */
        return PLCTAG_ERR_CREATE;
    }

    if(tag->read_in_progress) {
        return PLCTAG_STATUS_PENDING;
    }

    if(tag->write_in_progress) {
        return PLCTAG_STATUS_PENDING;
    }

    return tag->status;
}




int tag_tickler(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW, "Starting.");

    if(tag->read_in_progress) {
        pdebug(DEBUG_SPEW, "Read in progress.");
        rc = check_read_status(tag);
        tag->status = (int8_t)rc;

        /* check to see if the read finished. */
        if(!tag->read_in_progress) {
            /* read done */
            if(tag->first_read) {
                tag->first_read = 0;
                tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_CREATED, PLCTAG_STATUS_OK);
            }

            tag->read_complete = 1;
        }

        return rc;
    }

    if(tag->write_in_progress) {
        pdebug(DEBUG_SPEW, "Write in progress.");
        rc = check_write_status(tag);
        tag->status = (int8_t)rc;

        /* check to see if the write finished. */
        if(!tag->write_in_progress) {
            tag->write_complete = 1;
        }

        return rc;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return tag->status;
}


/*
 * tag_read_start
 *
 * This does not support multiple request fragments.
 */
int tag_read_start(ab_tag_p tag)
{
    pccc_dhp_co_req *pccc;
    uint8_t *data = NULL;
    uint8_t *embed_start = NULL;
    uint16_t conn_seq_id = (uint16_t)(session_get_new_seq_id(tag->session));
    int data_per_packet = 0;
    int overhead = 0;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p req = NULL;

    pdebug(DEBUG_INFO, "Starting");

    if(tag->read_in_progress || tag->write_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    tag->read_in_progress = 1;

    /* What is the overhead in the _response_ */
    overhead =   1  /* pccc command */
                 +1  /* pccc status */
                 +2;  /* pccc sequence num */

    data_per_packet = session_get_max_payload(tag->session) - overhead;

    if(data_per_packet <= 0) {
        tag->read_in_progress = 0;
        pdebug(DEBUG_WARN, "Unable to send request.  Packet overhead, %d bytes, is too large for packet, %d bytes!", overhead, session_get_max_payload(tag->session));
        return PLCTAG_ERR_TOO_LARGE;
    }

    if(data_per_packet < tag->size) {
        tag->read_in_progress = 0;
        pdebug(DEBUG_DETAIL, "Unable to send request: Tag size is %d, write overhead is %d, and write data per packet is %d!", tag->size, overhead, data_per_packet);
        return PLCTAG_ERR_TOO_LARGE;
    }

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if(rc != PLCTAG_STATUS_OK) {
        tag->read_in_progress = 0;
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    pccc = (pccc_dhp_co_req *)(req->data);

    /* point to the end of the struct */
    data = (req->data) + sizeof(pccc_dhp_co_req);

    /* set up the embedded PCCC packet */
    embed_start = (uint8_t*)(&pccc->cpf_conn_seq_num);

    /* copy encoded tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* encap fields */
    pccc->encap_command = h2le16(AB_EIP_CONNECTED_SEND);    /* ALWAYS 0x006F Unconnected Send*/

    /* router timeout */
    pccc->router_timeout = h2le16(1);                 /* one second timeout, enough? */

    /* Common Packet Format fields */
    pccc->cpf_item_count = h2le16(2);                 /* ALWAYS 2 */
    pccc->cpf_cai_item_type = h2le16(AB_EIP_ITEM_CAI);/* ALWAYS 0x00A1 connected address item */
    pccc->cpf_cai_item_length = h2le16(4);            /* ALWAYS 4 ? */
    pccc->cpf_cdi_item_type = h2le16(AB_EIP_ITEM_CDI);/* ALWAYS 0x00B1 - connected Data Item */
    pccc->cpf_cdi_item_length = h2le16((uint16_t)(data - embed_start)); /* REQ: fill in with length of remaining data. */
    pccc->cpf_conn_seq_num = h2le16(conn_seq_id);

    pdebug(DEBUG_DETAIL, "Total data length %d.", (int)(unsigned int)(data - (uint8_t*)(pccc)));
    pdebug(DEBUG_DETAIL, "Total payload length %d.", (int)(unsigned int)(data - embed_start));

    /* DH+ Routing */
    pccc->dest_link = h2le16(0);
    pccc->dest_node = h2le16(tag->session->dhp_dest);
    pccc->src_link = h2le16(0);
    pccc->src_node = h2le16(0) /*h2le16(tag->dhp_src)*/;

    /* fill in the PCCC command */
    pccc->pccc_command = AB_EIP_PCCC_TYPED_CMD;
    pccc->pccc_status = 0;  /* STS 0 in request */
    pccc->pccc_seq_num = h2le16(conn_seq_id);
    pccc->pccc_function = AB_EIP_SLC_RANGE_READ_FUNC;
    pccc->pccc_transfer_size = (uint8_t)(tag->elem_size * tag->elem_count); /* size to read/write in bytes. */

    /* get ready to add the request to the queue for this session */
    req->request_size = (int)(data - (req->data));

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if(rc != PLCTAG_STATUS_OK) {
        tag->read_in_progress = 0;
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        req->abort_request = 1;
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;
//    tag->status = PLCTAG_STATUS_PENDING;

    /* the read is now pending */
    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}



int tag_write_start(ab_tag_p tag)
{
    pccc_dhp_co_req *pccc;
    uint8_t *data;
    int data_per_packet = 0;
    int overhead = 0;
    uint16_t conn_seq_id = (uint16_t)(session_get_new_seq_id(tag->session));;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p req;

    pdebug(DEBUG_INFO, "Starting");

    if(tag->read_in_progress || tag->write_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    tag->write_in_progress = 1;

    /* how many packets will we need? How much overhead? */
    overhead = 2        /* size of sequence num */
               +8        /* DH+ routing */
               +1        /* DF1 command */
               +1        /* status */
               +2        /* PCCC packet sequence number */
               +1        /* PCCC function */
               +2        /* request offset */
               +2        /* tag size in elements */
               +(tag->encoded_name_size)
               +2;       /* this request size in elements */

    data_per_packet = session_get_max_payload(tag->session) - overhead;

    if(data_per_packet <= 0) {
        tag->write_in_progress = 0;
        pdebug(DEBUG_WARN, "Unable to send request.  Packet overhead, %d bytes, is too large for packet, %d bytes!", overhead, session_get_max_payload(tag->session));
        return PLCTAG_ERR_TOO_LARGE;
    }

    if(data_per_packet < tag->size) {
        tag->write_in_progress = 0;
        pdebug(DEBUG_WARN, "PCCC requests cannot be fragmented.  Too much data requested.");
        return PLCTAG_ERR_TOO_LARGE;
    }

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);

    if(rc != PLCTAG_STATUS_OK) {
        tag->write_in_progress = 0;
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    pccc = (pccc_dhp_co_req *)(req->data);

    /* point to the end of the struct */
    data = (req->data) + sizeof(pccc_dhp_co_req);

    /* copy laa into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* now copy the data to write */
    mem_copy(data, tag->data, tag->size);
    data += tag->size;

    /* now fill in the rest of the structure. */

    /* encap fields */
    pccc->encap_command = h2le16(AB_EIP_CONNECTED_SEND);    /* ALWAYS 0x006F Unconnected Send*/

    /* router timeout */
    pccc->router_timeout = h2le16(1);                 /* one second timeout, enough? */

    /* Common Packet Format fields */
    pccc->cpf_item_count = h2le16(2);                 /* ALWAYS 2 */
    pccc->cpf_cai_item_type = h2le16(AB_EIP_ITEM_CAI);/* ALWAYS 0x00A1 connected address item */
    pccc->cpf_cai_item_length = h2le16(4);            /* ALWAYS 4 ? */
    pccc->cpf_cdi_item_type = h2le16(AB_EIP_ITEM_CDI);/* ALWAYS 0x00B1 - connected Data Item */
    pccc->cpf_cdi_item_length = h2le16((uint16_t)(data - (uint8_t *)(&(pccc->cpf_conn_seq_num)))); /* REQ: fill in with length of remaining data. */
    pccc->cpf_conn_seq_num = h2le16(conn_seq_id);

    /* DH+ Routing */
    pccc->dest_link = h2le16(0);
    pccc->dest_node = h2le16(tag->session->dhp_dest);
    pccc->src_link = h2le16(0);
    pccc->src_node = h2le16(0) /*h2le16(tag->dhp_src)*/;

    // /* PCCC Command */
    // pccc->pccc_command = AB_EIP_PCCC_TYPED_CMD;
    // pccc->pccc_status = 0;  /* STS 0 in request */
    // pccc->pccc_seq_num = h2le16(conn_seq_id); /* FIXME - get sequence ID from session? */
    // pccc->pccc_function = AB_EIP_PLC5_RANGE_WRITE_FUNC;
    // pccc->pccc_transfer_offset = h2le16((uint16_t)0);
    // pccc->pccc_transfer_size = h2le16((uint16_t)((tag->size)/2));  /* size in 2-byte words */

    pccc->pccc_command = AB_EIP_PCCC_TYPED_CMD;
    pccc->pccc_status = 0;  /* STS 0 in request */
    pccc->pccc_seq_num = h2le16(conn_seq_id); /* FIXME - get sequence ID from session? */
    pccc->pccc_function = AB_EIP_SLC_RANGE_WRITE_FUNC;
    pccc->pccc_transfer_size = (uint8_t)(tag->size);

    /* get ready to add the request to the queue for this session */
    req->request_size = (int)(data - (req->data));

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if(rc != PLCTAG_STATUS_OK) {
        tag->write_in_progress = 0;
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        req->abort_request = 1;
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}


/*
 * check_read_status
 *
 * PCCC does not support request fragments.
 */
static int check_read_status(ab_tag_p tag)
{
    pccc_dhp_co_resp *resp;
    uint8_t *data;
    uint8_t *data_end;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_read_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Read request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    resp = (pccc_dhp_co_resp *)(request->data);

    /* point to the start of the data */
    data = (uint8_t *)resp + sizeof(*resp);

    /* point to the end of the data */
    data_end = (request->data + le2h16(resp->encap_length) + sizeof(eip_encap));

    /* fake exception */
    do {
        if(le2h16(resp->encap_command) != AB_EIP_CONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if(le2h32(resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(resp->pccc_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "PCCC command failed, response code: %d - %s", resp->pccc_status, pccc_decode_error(&resp->pccc_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        /* did we get the right amount of data? */
        if((data_end - data) != tag->size) {
            if((int)(data_end - data) > tag->size) {
                pdebug(DEBUG_WARN, "Too much data received!  Expected %d bytes but got %d bytes!", tag->size, (int)(data_end - data));
                rc = PLCTAG_ERR_TOO_LARGE;
            } else {
                pdebug(DEBUG_WARN, "Too little data received!  Expected %d bytes but got %d bytes!", tag->size, (int)(data_end - data));
                rc = PLCTAG_ERR_TOO_SMALL;
            }
            break;
        }

        /* copy data into the tag. */
        mem_copy(tag->data, data, (int)(data_end - data));

        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request. */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    tag->read_in_progress = 0;

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}


static int check_write_status(ab_tag_p tag)
{
    pccc_dhp_co_resp *pccc_resp;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_write_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Write request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    pccc_resp = (pccc_dhp_co_resp *)(request->data);

    /* fake exception */
    do {
        /* check the response status */
        if( le2h16(pccc_resp->encap_command) != AB_EIP_CONNECTED_SEND) {
            pdebug(DEBUG_WARN, "EIP unexpected response packet type: %d!", pccc_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if(le2h32(pccc_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(pccc_resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc_resp->pccc_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "PCCC command failed, response code: %d - %s", pccc_resp->pccc_status, pccc_decode_error(&pccc_resp->pccc_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        /* everything OK */
        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request. */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    tag->write_in_progress = 0;

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}


#endif // __PROTOCOLS_AB_EIP_SLC_DHP_C__


#ifndef __PROTOCOLS_AB_EIP_SLC_PCCC_C__
#define __PROTOCOLS_AB_EIP_SLC_PCCC_C__

static int tag_read_start(ab_tag_p tag);
static int tag_status(ab_tag_p tag);
static int tag_tickler(ab_tag_p tag);
static int tag_write_start(ab_tag_p tag);

struct tag_vtable_t slc_vtable = {
    (tag_vtable_func)ab_tag_abort, /* shared */
    (tag_vtable_func)tag_read_start,
    (tag_vtable_func)tag_status,
    (tag_vtable_func)tag_tickler,
    (tag_vtable_func)tag_write_start,
    (tag_vtable_func)NULL, /* wake_plc */

    /* data accessors */
    ab_get_int_attrib,
    ab_set_int_attrib
};


/* default string types used for PLC-5 PLCs. */
tag_byte_order_t slc_tag_byte_order = {
    .is_allocated = 0,

    .str_is_defined = 1,
    .str_is_counted = 1,
    .str_is_fixed_length = 1,
    .str_is_zero_terminated = 0,
    .str_is_byte_swapped = 1,

    .str_count_word_bytes = 2,
    .str_max_capacity = 82,
    .str_total_length = 84,
    .str_pad_bytes = 0,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};




static int check_read_status(ab_tag_p tag);
static int check_write_status(ab_tag_p tag);




START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;         /* ALWAYS 0x006f Unconnected Send*/
    uint16_le encap_length;          /* packet size in bytes - 24 */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;  /* whatever we want to set this to, used for
                                     * identifying responses when more than one
                                     * are in flight at once.
                                     */
    uint32_le encap_options;         /* 0, reserved for future use */

    /* Interface Handle etc. */
    uint32_le interface_handle;      /* ALWAYS 0 */
    uint16_le router_timeout;        /* in seconds, 5 or 10 seems to be good.*/

    /* Common Packet Format - CPF Unconnected */
    uint16_le cpf_item_count;        /* ALWAYS 2 */
    uint16_le cpf_nai_item_type;     /* ALWAYS 0 */
    uint16_le cpf_nai_item_length;   /* ALWAYS 0 */
    uint16_le cpf_udi_item_type;     /* ALWAYS 0x00B2 - Unconnected Data Item */
    uint16_le cpf_udi_item_length;   /* REQ: fill in with length of remaining data. */

    /* PCCC Command Req Routing */
    uint8_t service_code;           /* ALWAYS 0x4B, Execute PCCC */
    uint8_t req_path_size;          /* ALWAYS 0x02, in 16-bit words */
    uint8_t req_path[4];            /* ALWAYS 0x20,0x67,0x24,0x01 for PCCC */
    uint8_t request_id_size;        /* ALWAYS 7 */
    uint16_le vendor_id;            /* Our CIP Vendor ID */
    uint32_le vendor_serial_number; /* Our CIP Vendor Serial Number */

    /* PCCC Command */
    uint8_t pccc_command;           /* CMD read, write etc. */
    uint8_t pccc_status;            /* STS 0x00 in request */
    uint16_le pccc_seq_num;         /* TNS transaction/sequence id */
    uint8_t pccc_function;          /* FNC sub-function of command */
    uint8_t pccc_transfer_size;     /* total number of bytes requested */
} END_PACK pccc_req;






/*
 * tag_status
 *
 * Get the tag status.
 */

int tag_status(ab_tag_p tag)
{
    if (!tag->session) {
        /* this is not OK.  This is fatal! */
        return PLCTAG_ERR_CREATE;
    }

    if(tag->read_in_progress) {
        return PLCTAG_STATUS_PENDING;
    }

    if(tag->write_in_progress) {
        return PLCTAG_STATUS_PENDING;
    }

    return tag->status;
}


int tag_tickler(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW, "Starting.");

    if(tag->read_in_progress) {
        pdebug(DEBUG_SPEW, "Read in progress.");
        rc = check_read_status(tag);
        tag->status = (int8_t)rc;

        /* check to see if the read finished. */
        if(!tag->read_in_progress) {
            /* read done */
            if(tag->first_read) {
                tag->first_read = 0;
                tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_CREATED, PLCTAG_STATUS_OK);
            }

            tag->read_complete = 1;
        }

        return rc;
    }

    if(tag->write_in_progress) {
        pdebug(DEBUG_SPEW, "Write in progress.");
        rc = check_write_status(tag);
        tag->status = (int8_t)rc;

        /* check to see if the write finished. */
        if(!tag->write_in_progress) {
            tag->write_complete = 1;
        }

        return rc;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return tag->status;

}


/*
 * tag_read_start
 *
 * Start a PCCC tag read (SLC).
 */

int tag_read_start(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    ab_request_p req;
    uint16_t conn_seq_id = (uint16_t)(session_get_new_seq_id(tag->session));;
    int overhead;
    int data_per_packet;
    pccc_req *pccc;
    uint8_t *data;
    uint8_t *embed_start;

    pdebug(DEBUG_INFO,"Starting");

    if(tag->read_in_progress || tag->write_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    tag->read_in_progress = 1;

    /* how many packets will we need? How much overhead? */
    //overhead = sizeof(pccc_resp) + 4 + tag->encoded_name_size; /* MAGIC 4 = fudge */

    /* calculate based on the response. */
    overhead =   1      /* PCCC CMD */
                +1      /* PCCC status */
                +2;     /* PCCC packet sequence number */

    data_per_packet = session_get_max_payload(tag->session) - overhead;

    if(data_per_packet <= 0) {
        pdebug(DEBUG_WARN,"Unable to send request.  Packet overhead, %d bytes, is too large for packet, %d bytes!", overhead, session_get_max_payload(tag->session));
        tag->read_in_progress = 0;
        return PLCTAG_ERR_TOO_LARGE;
    }

    if(data_per_packet < tag->size) {
        pdebug(DEBUG_DETAIL,"Unable to send request: Tag size is %d, write overhead is %d, and write data per packet is %d!", tag->size, overhead, data_per_packet);
        tag->read_in_progress = 0;
        return PLCTAG_ERR_TOO_LARGE;
    }

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to get new request.  rc=%d",rc);
        tag->read_in_progress = 0;
        return rc;
    }

    /* point the struct pointers to the buffer*/
    pccc = (pccc_req*)(req->data);

    /* point to the end of the struct */
    data = ((uint8_t *)pccc) + sizeof(pccc_req);

    /* set up the embedded PCCC packet */
    embed_start = (uint8_t*)(&pccc->service_code);

    /* copy encoded tag name into the request */
    mem_copy(data,tag->encoded_name,tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* fill in the fixed fields. */

    /* encap fields */
    pccc->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND);    /* set up for unconnected sending */

    /* router timeout */
    pccc->router_timeout = h2le16(1);                 /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    pccc->cpf_item_count        = h2le16(2);                /* ALWAYS 2 */
    pccc->cpf_nai_item_type     = h2le16(AB_EIP_ITEM_NAI);  /* ALWAYS 0 */
    pccc->cpf_nai_item_length   = h2le16(0);                /* ALWAYS 0 */
    pccc->cpf_udi_item_type     = h2le16(AB_EIP_ITEM_UDI);  /* ALWAYS 0x00B2 - Unconnected Data Item */
    pccc->cpf_udi_item_length   = h2le16((uint16_t)(data - embed_start));  /* REQ: fill in with length of remaining data. */

    /* Command Routing */
    pccc->service_code = AB_EIP_CMD_PCCC_EXECUTE;  /* ALWAYS 0x4B, Execute PCCC */
    pccc->req_path_size = 2;   /* ALWAYS 2, size in words of path, next field */
    pccc->req_path[0] = 0x20;  /* class */
    pccc->req_path[1] = 0x67;  /* PCCC Execute */
    pccc->req_path[2] = 0x24;  /* instance */
    pccc->req_path[3] = 0x01;  /* instance 1 */

    /* PCCC ID */
    pccc->request_id_size = 7;                              /* ALWAYS 7 */
    pccc->vendor_id = h2le16(AB_EIP_VENDOR_ID);             /* Our CIP Vendor */
    pccc->vendor_serial_number = h2le32(AB_EIP_VENDOR_SN);  /* our unique serial number */

    /* fill in the PCCC command */
    pccc->pccc_command = AB_EIP_PCCC_TYPED_CMD;
    pccc->pccc_status = 0;  /* STS 0 in request */
    pccc->pccc_seq_num = h2le16(conn_seq_id);
    pccc->pccc_function = AB_EIP_SLC_RANGE_READ_FUNC;
    pccc->pccc_transfer_size = (uint8_t)(tag->elem_size * tag->elem_count); /* size to read/write in bytes. */

    /*
     * after the embedded packet, we need to tell the message router
     * how to get to the target device.
     */

    /* set the size of the request */
    req->request_size = (int)(data - (req->data));

    /* mark it as ready to send */
    //req->send_request = 1;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        req->abort_request = 1;
        tag->read_in_progress = 0;

        tag->req = (ab_request_p)rc_dec(req);

        return rc;
    }

    /* save the request for later */
    tag->req = req;
//    tag->status = PLCTAG_STATUS_PENDING;

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}





/*
 * check_read_status
 *
 * NOTE that we can have only one outstanding request because PCCC
 * does not support fragments.
 */


static int check_read_status(ab_tag_p tag)
{
    pccc_resp *pccc;
    uint8_t *data;
    uint8_t *data_end;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_read_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Read request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    pccc = (pccc_resp*)(request->data);

    /* point to the start of the data */
    data = (uint8_t *)pccc + sizeof(*pccc);

    data_end = (request->data + le2h16(pccc->encap_length) + sizeof(eip_encap));

    /* fake exceptions */
    do {
        if(le2h16(pccc->encap_command) != AB_EIP_UNCONNECTED_SEND) {
            pdebug(DEBUG_WARN,"Unexpected EIP packet type received: %d!",pccc->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if(le2h32(pccc->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN,"EIP command failed, response code: %d",le2h32(pccc->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc->general_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN,"PCCC command failed, response code: (%d) %s", pccc->general_status, decode_cip_error_long((uint8_t*)&(pccc->general_status)));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc->pccc_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "PCCC command failed, response code: %d - %s", pccc->pccc_status, pccc_decode_error(&pccc->pccc_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        /* did we get the right amount of data? */
        if((data_end - data) != tag->size) {
            if((int)(data_end - data) > tag->size) {
                pdebug(DEBUG_WARN,"Too much data received!  Expected %d bytes but got %d bytes!", tag->size, (int)(data_end - data));
                rc = PLCTAG_ERR_TOO_LARGE;
            } else {
                pdebug(DEBUG_WARN,"Too little data received!  Expected %d bytes but got %d bytes!", tag->size, (int)(data_end - data));
                rc = PLCTAG_ERR_TOO_SMALL;
            }
            break;
        }

        /* copy data into the tag. */
        mem_copy(tag->data, data, (int)(data_end - data));

        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request. */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    tag->read_in_progress = 0;

    pdebug(DEBUG_SPEW,"Done.");

    return rc;
}





/* FIXME  convert to unconnected messages. */

int tag_write_start(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    pccc_req *pccc;
    uint8_t *data;
    uint16_t conn_seq_id = (uint16_t)(session_get_new_seq_id(tag->session));
    uint8_t *embed_start;
    int overhead, data_per_packet;
    ab_request_p req = NULL;

    pdebug(DEBUG_INFO,"Starting.");

    if(tag->read_in_progress || tag->write_in_progress) {
        pdebug(DEBUG_WARN, "Read or write operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    tag->write_in_progress = 1;

    /* overhead comes from the request*/
    overhead =    1  /* PCCC command */
                 +1  /* PCCC status */
                 +2  /* PCCC sequence number */
                 +1  /* PCCC function */
                 +1  /* request total transfer size in bytes. */
                 + (tag->encoded_name_size);

    data_per_packet = session_get_max_payload(tag->session) - overhead;

    if(data_per_packet <= 0) {
        pdebug(DEBUG_WARN,"Unable to send request.  Packet overhead, %d bytes, is too large for packet, %d bytes!", overhead, session_get_max_payload(tag->session));
        tag->write_in_progress =0;
        return PLCTAG_ERR_TOO_LARGE;
    }

    if(data_per_packet < tag->size) {
        pdebug(DEBUG_DETAIL,"Tag size is %d, write overhead is %d, and write data per packet is %d.", session_get_max_payload(tag->session), overhead, data_per_packet);
        tag->write_in_progress =0;
        return PLCTAG_ERR_TOO_LARGE;
    }

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to get new request.  rc=%d",rc);
        tag->write_in_progress =0;
        return rc;
    }

    pccc = (pccc_req*)(req->data);

    /* set up the embedded PCCC packet */
    embed_start = (uint8_t*)(&pccc->service_code);

    /* point to the end of the struct */
    data = (req->data) + sizeof(pccc_req);

    /* copy encoded tag name into the request */
    mem_copy(data,tag->encoded_name,tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* write the mask if this is a bit tag */
    if(tag->is_bit) {
        for(int i=0; i < tag->elem_size; i++) {
            if((tag->bit / 8) == i) {
                *data = (uint8_t)(1 << (tag->bit % 8));

                pdebug(DEBUG_DETAIL, "adding mask byte %d: %x", i, *data);

                data++;
            } else {
                /* this is not the data we care about. */
                *data = (uint8_t)0x00;

                pdebug(DEBUG_DETAIL, "adding mask byte %d: %x", i, *data);

                data++;
            }
        }
    }

    /* now copy the data to write */
    mem_copy(data,tag->data,tag->size);
    data += tag->size;

    /* now fill in the rest of the structure. */

    /* encap fields */
    pccc->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND);

    /* router timeout */
    pccc->router_timeout = h2le16(1);                 /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    pccc->cpf_item_count        = h2le16(2);                /* ALWAYS 2 */
    pccc->cpf_nai_item_type     = h2le16(AB_EIP_ITEM_NAI);  /* ALWAYS 0 */
    pccc->cpf_nai_item_length   = h2le16(0);                /* ALWAYS 0 */
    pccc->cpf_udi_item_type     = h2le16(AB_EIP_ITEM_UDI);  /* ALWAYS 0x00B2 - Unconnected Data Item */
    pccc->cpf_udi_item_length   = h2le16((uint16_t)(data - embed_start));  /* REQ: fill in with length of remaining data. */

    pdebug(DEBUG_DETAIL, "Total data length %d.", (int)(unsigned int)(data - (uint8_t*)(pccc)));
    pdebug(DEBUG_DETAIL, "Total payload length %d.", (int)(unsigned int)(data - embed_start));

    /* Command Routing */
    pccc->service_code = AB_EIP_CMD_PCCC_EXECUTE;  /* ALWAYS 0x4B, Execute PCCC */
    pccc->req_path_size = 2;   /* ALWAYS 2, size in words of path, next field */
    pccc->req_path[0] = 0x20;  /* class */
    pccc->req_path[1] = 0x67;  /* PCCC Execute */
    pccc->req_path[2] = 0x24;  /* instance */
    pccc->req_path[3] = 0x01;  /* instance 1 */

    /* PCCC ID */
    pccc->request_id_size = 7;  /* ALWAYS 7 */
    pccc->vendor_id = h2le16(AB_EIP_VENDOR_ID);                 /* Our CIP Vendor */
    pccc->vendor_serial_number = h2le32(AB_EIP_VENDOR_SN);      /* our unique serial number */

    /* PCCC Command */
    pccc->pccc_command = AB_EIP_PCCC_TYPED_CMD;
    pccc->pccc_status = 0;  /* STS 0 in request */
    pccc->pccc_seq_num = h2le16(conn_seq_id); /* FIXME - get sequence ID from session? */
    pccc->pccc_function = (tag->is_bit ? AB_EIP_SLC_RANGE_BIT_WRITE_FUNC : AB_EIP_SLC_RANGE_WRITE_FUNC);
    pccc->pccc_transfer_size = (uint8_t)(tag->size);

    /* get ready to add the request to the queue for this session */
    req->request_size = (int)(data - (req->data));

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        req->abort_request = 1;
        tag->write_in_progress =0;

        tag->req = (ab_request_p)rc_dec(req);

        return rc;
    }

    /* the write is now pending */
    tag->write_in_progress = 1;

    /* save the request for later */
    tag->req = req;

    /* save the request for later */
    tag->req = req;

//    tag->status = PLCTAG_STATUS_PENDING;

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}



/*
 * check_write_status
 *
 * Fragments are not supported.
 */
static int check_write_status(ab_tag_p tag)
{
    pccc_resp *pccc;
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_write_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Write request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    pccc = (pccc_resp*)(request->data);

    /* fake exception */
    do {
        /* check the response status */
        if( le2h16(pccc->encap_command) != AB_EIP_UNCONNECTED_SEND) {
            pdebug(DEBUG_WARN,"EIP unexpected response packet type: %d!",pccc->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if(le2h32(pccc->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN,"EIP command failed, response code: %d",le2h32(pccc->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc->general_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN,"PCCC command failed, response code: %d",pccc->general_status);
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(pccc->pccc_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "PCCC command failed, response code: %d - %s", pccc->pccc_status, pccc_decode_error(&pccc->pccc_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request. */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);
    
    tag->write_in_progress = 0;

    pdebug(DEBUG_SPEW,"Done.");

    /* Success! */
    return rc;
}

#endif // __PROTOCOLS_AB_EIP_SLC_PCCC_C__


#ifndef __PROTOCOLS_AB_PCCC_C__
#define __PROTOCOLS_AB_PCCC_C__

#include <ctype.h>
#include <limits.h>
#include <float.h>

//static int parse_pccc_logical_address(const char *name, pccc_file_t address->file_type, int *file_num, int *elem_num, int *sub_elem_num);
static int parse_pccc_file_type(const char **str, pccc_addr_t *address);
static int parse_pccc_file_num(const char **str, pccc_addr_t *address);
static int parse_pccc_elem_num(const char **str, pccc_addr_t *address);
static int parse_pccc_subelem_mnemonic(const char **str, pccc_addr_t *address);
static int parse_pccc_bit_num(const char **str, pccc_addr_t *address);
static void encode_data(uint8_t *data, int *index, int val);
// static int encode_file_type(pccc_file_t file_type);



/*
 * Public functions
 */



/*
 * The PLC/5, SLC, and MicroLogix have their own unique way of determining what you are
 * accessing.   All variables and data are in data-table files.  Each data-table contains
 * data of one type.
 *
 * The main ones are:
 *
 * File Type   Name    Structure/Size
 * ASCII       A       1 byte per character
 * Bit         B       word
 * Block-transfer BT   structure/6 words
 * Counter     C       structure/3 words
 * BCD         D       word
 * Float       F       32-bit float
 * Input       I       word
 * Output      O       word
 * Long Int    L       32-bit integer, two words
 * Message     MG      structure/56 words
 * Integer     N       word
 * PID         PD      structure/41 floats
 * Control     R       structure/3 words
 * Status      S       word
 * SFC         SC      structure/3 words
 * String      ST      structure/84 bytes
 * Timer       T       structure/3 words
 *
 * Logical address are formulated as follows:
 *
 * <data type> <file #> : <element #> <sub-element selector>
 *
 * The data type is the "Name" above.
 *
 * The file # is the data-table file number.
 *
 * The element number is the index (zero based) into the table.
 *
 * The sub-element selector is one of three things:
 *
 * 1) missing.   A logical address like N7:0 is perfectly fine and selects a full 16-bit integer.
 * 2) a named field in a timer, counter, control, PID, block transfer, message, or string.
 * 3) a bit within a word for integers or bits.
 *
 * The following are the supported named fields:
 *
 * Timer/Counter
 * Offset   Field
 * 0        con - control
 * 1        pre - preset
 * 2        acc - accumulated
 *
 * R/Control
 * Offset   Field
 * 0        con - control
 * 1        len - length
 * 2        pos - position
 *
 * PD/PID
 * Offset   Field
 * 0        con - control
 * 2        sp - SP
 * 4        kp - Kp
 * 6        ki - Ki
 * 8        kd - Kd
 * 26       pv - PV
 *
 * BT
 * Offset   Field
 * 0        con - control
 * 1        rlen - RLEN
 * 2        dlen - DLEN
 * 3        df - data file #
 * 4        elem - element #
 * 5        rgs - rack/grp/slot
 *
 * MG
 * Offset   Field
 * 0        con - control
 * 1        err - error
 * 2        rlen - RLEN
 * 3        dlen - DLEN
 *
 * ST
 * Offset   Field
 * 0        LEN - length
 * 2-84     DATA - characters of string
 */



/*
 * parse_pccc_logical_address
 *
 * Parse the address into a structure with all the fields broken out.  This
 * checks the validity of the address in a PLC-neutral way.
 */

int parse_pccc_logical_address(const char *file_address, pccc_addr_t *address)
{
    int rc = PLCTAG_STATUS_OK;
    const char *p = file_address;

    pdebug(DEBUG_DETAIL, "Starting.");

    do {
        if((rc = parse_pccc_file_type(&p, address)) != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Unable to parse PCCC-style tag for data-table type! Error %s!", plc_tag_decode_error(rc));
            break;
        }

        if((rc = parse_pccc_file_num(&p, address)) != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Unable to parse PCCC-style tag for file number! Error %s!", plc_tag_decode_error(rc));
            break;
        }

        if((rc = parse_pccc_elem_num(&p, address)) != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Unable to parse PCCC-style tag for element number! Error %s!", plc_tag_decode_error(rc));
            break;
        }

        if((rc = parse_pccc_subelem_mnemonic(&p, address)) != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Unable to parse PCCC-style tag for subelement number! Error %s!", plc_tag_decode_error(rc));
            break;
        }

        if((rc = parse_pccc_bit_num(&p, address)) != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Unable to parse PCCC-style tag for subelement number! Error %s!", plc_tag_decode_error(rc));
            break;
        }
    } while(0);

    pdebug(DEBUG_DETAIL, "Starting.");

    return rc;
}




/*
 * Encode the logical address as a level encoding for use with PLC/5 PLCs.
 *
 * Byte Meaning
 * 0    level flags
 * 1-3  level one
 * 1-3  level two
 * 1-3  level three
 */

int plc5_encode_address(uint8_t *data, int *size, int buf_size, pccc_addr_t *address)
{
    uint8_t level_byte = 0;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!data || !size || !address) {
        pdebug(DEBUG_WARN, "Called with null data, or name or zero sized data!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* zero out the stored size. */
    *size = 0;

    /* check for space. */
    if(buf_size < (1 + 3 + 3 + 3)) {
        pdebug(DEBUG_WARN,"Encoded PCCC logical address buffer is too small!");
        return PLCTAG_ERR_TOO_SMALL;
    }

    /* allocate space for the level byte */
    *size = 1;

    /* do the required levels.  Remember we start at the low bit! */
    level_byte = 0x06; /* level one and two */

    /* add in the data file number. */
    encode_data(data, size, address->file);

    /* add in the element number */
    encode_data(data, size, address->element);

    /* check to see if we need to put in a subelement. */
    if(address->sub_element >= 0) {
        level_byte |= 0x08;

        encode_data(data, size, address->sub_element);
    }

    /* store the encoded levels. */
    data[0] = level_byte;

    pdebug(DEBUG_DETAIL, "PLC/5 encoded address:");
    pdebug_dump_bytes(DEBUG_DETAIL, data, *size);

    pdebug(DEBUG_DETAIL,"Done.");

    return PLCTAG_STATUS_OK;
}




/*
 * Encode the logical address as a file/type/element/subelement struct.
 *
 * element  Meaning
 * file     Data file #.
 * type     Data file type.
 * element  element # within data file.
 * sub      field/sub-element within data file for structured data.
 */

int slc_encode_address(uint8_t *data, int *size, int buf_size, pccc_addr_t *address)
{
    pdebug(DEBUG_DETAIL, "Starting.");

    if(!data || !size) {
        pdebug(DEBUG_WARN, "Called with null data, or name or zero sized data!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* check for space. */
    if(buf_size < (3 + 1 + 3 + 3)) {
        pdebug(DEBUG_WARN,"Encoded SLC logical address buffer is too small!");
        return PLCTAG_ERR_TOO_SMALL;
    }

    /* zero out the size */
    *size = 0;

    if(address->file_type == 0) {
        pdebug(DEBUG_WARN,"SLC file type %d cannot be decoded!", address->file_type);
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* encode the file number */
    encode_data(data, size, address->file);

    /* encode the data file type. */
    encode_data(data, size, (int)address->file_type);

    /* add in the element number */
    encode_data(data, size, address->element);

    /* add in the sub-element number */
    encode_data(data, size, (address->sub_element < 0 ? 0 : address->sub_element));

    pdebug(DEBUG_DETAIL, "SLC/Micrologix encoded address:");
    pdebug_dump_bytes(DEBUG_DETAIL, data, *size);

    pdebug(DEBUG_DETAIL,"Done.");

    return PLCTAG_STATUS_OK;
}



uint8_t pccc_calculate_bcc(uint8_t *data,int size)
{
    int bcc = 0;
    int i;

    for(i = 0; i < size; i++)
        bcc += data[i];

    /* we want the twos-compliment of the lowest 8 bits. */
    bcc = -bcc;

    /* only the lowest 8 bits */
    return (uint8_t)(bcc & 0xFF);
}





/* Calculate AB's version of CRC-16.  We use a precalculated
 * table for simplicity.   Note that modern processors execute
 * so many instructions per second, that using a table, even
 * this small, is probably slower.
 */

uint16_t CRC16Bytes[] = {
    0x0000, 0xC0C1, 0xC181, 0x0140, 0xC301, 0x03C0, 0x0280, 0xC241,
    0xC601, 0x06C0, 0x0780, 0xC741, 0x0500, 0xC5C1, 0xC481, 0x0440,
    0xCC01, 0x0CC0, 0x0D80, 0xCD41, 0x0F00, 0xCFC1, 0xCE81, 0x0E40,
    0x0A00, 0xCAC1, 0xCB81, 0x0B40, 0xC901, 0x09C0, 0x0880, 0xC841,
    0xD801, 0x18C0, 0x1980, 0xD941, 0x1B00, 0xDBC1, 0xDA81, 0x1A40,
    0x1E00, 0xDEC1, 0xDF81, 0x1F40, 0xDD01, 0x1DC0, 0x1C80, 0xDC41,
    0x1400, 0xD4C1, 0xD581, 0x1540, 0xD701, 0x17C0, 0x1680, 0xD641,
    0xD201, 0x12C0, 0x1380, 0xD341, 0x1100, 0xD1C1, 0xD081, 0x1040,
    0xF001, 0x30C0, 0x3180, 0xF141, 0x3300, 0xF3C1, 0xF281, 0x3240,
    0x3600, 0xF6C1, 0xF781, 0x3740, 0xF501, 0x35C0, 0x3480, 0xF441,
    0x3C00, 0xFCC1, 0xFD81, 0x3D40, 0xFF01, 0x3FC0, 0x3E80, 0xFE41,
    0xFA01, 0x3AC0, 0x3B80, 0xFB41, 0x3900, 0xF9C1, 0xF881, 0x3840,
    0x2800, 0xE8C1, 0xE981, 0x2940, 0xEB01, 0x2BC0, 0x2A80, 0xEA41,
    0xEE01, 0x2EC0, 0x2F80, 0xEF41, 0x2D00, 0xEDC1, 0xEC81, 0x2C40,
    0xE401, 0x24C0, 0x2580, 0xE541, 0x2700, 0xE7C1, 0xE681, 0x2640,
    0x2200, 0xE2C1, 0xE381, 0x2340, 0xE101, 0x21C0, 0x2080, 0xE041,
    0xA001, 0x60C0, 0x6180, 0xA141, 0x6300, 0xA3C1, 0xA281, 0x6240,
    0x6600, 0xA6C1, 0xA781, 0x6740, 0xA501, 0x65C0, 0x6480, 0xA441,
    0x6C00, 0xACC1, 0xAD81, 0x6D40, 0xAF01, 0x6FC0, 0x6E80, 0xAE41,
    0xAA01, 0x6AC0, 0x6B80, 0xAB41, 0x6900, 0xA9C1, 0xA881, 0x6840,
    0x7800, 0xB8C1, 0xB981, 0x7940, 0xBB01, 0x7BC0, 0x7A80, 0xBA41,
    0xBE01, 0x7EC0, 0x7F80, 0xBF41, 0x7D00, 0xBDC1, 0xBC81, 0x7C40,
    0xB401, 0x74C0, 0x7580, 0xB541, 0x7700, 0xB7C1, 0xB681, 0x7640,
    0x7200, 0xB2C1, 0xB381, 0x7340, 0xB101, 0x71C0, 0x7080, 0xB041,
    0x5000, 0x90C1, 0x9181, 0x5140, 0x9301, 0x53C0, 0x5280, 0x9241,
    0x9601, 0x56C0, 0x5780, 0x9741, 0x5500, 0x95C1, 0x9481, 0x5440,
    0x9C01, 0x5CC0, 0x5D80, 0x9D41, 0x5F00, 0x9FC1, 0x9E81, 0x5E40,
    0x5A00, 0x9AC1, 0x9B81, 0x5B40, 0x9901, 0x59C0, 0x5880, 0x9841,
    0x8801, 0x48C0, 0x4980, 0x8941, 0x4B00, 0x8BC1, 0x8A81, 0x4A40,
    0x4E00, 0x8EC1, 0x8F81, 0x4F40, 0x8D01, 0x4DC0, 0x4C80, 0x8C41,
    0x4400, 0x84C1, 0x8581, 0x4540, 0x8701, 0x47C0, 0x4680, 0x8641,
    0x8201, 0x42C0, 0x4380, 0x8341, 0x4100, 0x81C1, 0x8081, 0x4040
};


uint16_t pccc_calculate_crc16(uint8_t *data, int size)
{
    uint16_t running_crc = 0;
    int i;

    /* for each byte in the data... */
    for(i=0; i < size; i++) {
        /* calculate the running byte.  This is a lot like
         * a CBC.  You keep the running value as you go along
         * and the table is precalculated to have all the right
         * 256 values.
         */

        /* mask the CRC and XOR with the data byte */
        uint8_t running_byte = (uint8_t)(running_crc & 0x00FF) ^ data[i];

        /* calculate the next CRC value by shifting and XORing with
         * the value we get from a table lookup using the running
         * byte as an index.  This chains the data forward as we
         * calculate the CRC.
         */
        running_crc = (running_crc >> 8) ^ CRC16Bytes[running_byte];
    }

    return running_crc;
}






const char *pccc_decode_error(uint8_t *error_ptr)
{
    uint8_t error = *error_ptr;

    /* extended error? */
    if(error == 0xF0) {
        error = *(error_ptr + 3);
    }

    switch(error) {
    case 1:
        return "Error converting block address.";
        break;

    case 2:
        return "Less levels specified in address than minimum for any address.";
        break;

    case 3:
        return "More levels specified in address than system supports";
        break;

    case 4:
        return "Symbol not found.";
        break;

    case 5:
        return "Symbol is of improper format.";
        break;

    case 6:
        return "Address doesn't point to something usable.";
        break;

    case 7:
        return "File is wrong size.";
        break;

    case 8:
        return "Cannot complete request, situation has changed since the start of the command.";
        break;

    case 9:
        return "File is too large.";
        break;

    case 0x0A:
        return "Transaction size plus word address is too large.";
        break;

    case 0x0B:
        return "Access denied, improper privilege.";
        break;

    case 0x0C:
        return "Condition cannot be generated - resource is not available (some has upload active)";
        break;

    case 0x0D:
        return "Condition already exists - resource is already available.";
        break;

    case 0x0E:
        return "Command could not be executed PCCC decode error.";
        break;

    case 0x0F:
        return "Requester does not have upload or download access - no privilege.";
        break;

    case 0x10:
        return "Illegal command or format.";
        break;

    case 0x20:
        return "Host has a problem and will not communicate.";
        break;

    case 0x30:
        return "Remote node host is missing, disconnected, or shut down.";
        break;

    case 0x40:
        return "Host could not complete function due to hardware fault.";
        break;

    case 0x50:
        return "Addressing problem or memory protect rungs.";
        break;

    case 0x60:
        return "Function not allowed due to command protection selection.";
        break;

    case 0x70:
        return "Processor is in Program mode.";
        break;

    case 0x80:
        return "Compatibility mode file missing or communication zone problem.";
        break;

    case 0x90:
        return "Remote node cannot buffer command.";
        break;

    case 0xA0:
        return "Wait ACK (1775-KA buffer full).";
        break;

    case 0xB0:
        return "Remote node problem due to download.";
        break;

    case 0xC0:
        return "Wait ACK (1775-KA buffer full).";  /* why is this duplicate? */
        break;

    default:
        return "Unknown error response.";
        break;
    }


    return "Unknown error response.";
}





/*
 * FIXME This does not check for data overruns!
 */

uint8_t *pccc_decode_dt_byte(uint8_t *data,int data_size, int *pccc_res_type, int *pccc_res_length)
{
    uint32_t d_type;
    uint32_t d_size;

    /* check the data size.  If it is too small, then
     * we probably have an empty result.  The smallest valid
     * size is probably more than two bytes.  One for the DT
     * byte and another for the data itself.
     */
    if(data_size < 2) {
        *pccc_res_type = 0;
        *pccc_res_length = 0;
        return NULL;
    }

    /* get the type and data size parts */
    d_type = (((uint32_t)(*data) & (uint32_t)0xF0)>>(uint32_t)4);
    d_size = (*data) & 0x0F;

    /* check the type.  If it is too large to hold in
     * the bottom three bits of the nybble, then the
     * top bit will be set and the bottom three will
     * hold the number of bytes that follows for the
     * type value.  We stop after 4 bytes.  Hopefully
     * that works.
     */

    if(d_type & 0x08) {
        int size_bytes = d_type & 0x07;

        if(size_bytes > 4) {
            return NULL;
        }

        d_type = 0;

        while(size_bytes--) {
            data++; /* we leave the pointer at the last read byte */
            d_type <<= 8;
            d_type |= *data;
        }
    }

    /* same drill for the size */
    if(d_size & 0x08) {
        int size_bytes = d_size & 0x07;

        if(size_bytes > 4) {
            return NULL;
        }

        d_size = 0;

        while(size_bytes--) {
            data++; /* we leave the pointer at the last read byte */
            d_size <<= 8;
            d_size |= *data;
        }
    }

    *pccc_res_type = (int)d_type;
    *pccc_res_length = (int)d_size;

    /* point past the last byte read */
    data++;

    return data;
}





int pccc_encode_dt_byte(uint8_t *data,int buf_size, uint32_t data_type, uint32_t data_size)
{
    uint8_t *dt_byte = data;
    uint8_t d_byte;
    uint8_t t_byte;
    int size_bytes;

    /* increment past the dt_byte */
    data++;
    buf_size--;

    /* if data type fits in 3 bits then
     * just use the dt_byte.
     */

    if(data_type <= 0x07) {
        t_byte = (uint8_t)data_type;
        data_type =0;
    } else {
        size_bytes=0;

        while((data_type & 0xFF) && data_size) {
            *data = data_type & 0xFF;
            data_type >>= 8;
            size_bytes++;
            buf_size--;
            data++;
        }

        t_byte = (uint8_t)(0x08 | size_bytes);
    }

    if(data_size <= 0x07) {
        d_byte = (uint8_t)data_size;
        data_size = 0;
    } else {
        size_bytes = 0;

        while((data_size & 0xFF) && data_size) {
            *data = data_size & 0xFF;
            data_size >>= 8;
            size_bytes++;
            buf_size--;
            data++;
        }

        d_byte = (uint8_t)(0x08 | size_bytes);
    }

    *dt_byte = (uint8_t)((t_byte << 4) | d_byte);

    /* did we succeed? */
    if(buf_size == 0 || data_type != 0 || data_size != 0)
        return 0;


    return (int)(data - dt_byte);
}





/*
 * Helper functions
 */


/*
 * FIXME TODO - refactor this into a table-driven function.
 */

int parse_pccc_file_type(const char **str, pccc_addr_t *address)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    switch((*str)[0]) {
    case 'A':
    case 'a': /* ASCII */
        pdebug(DEBUG_DETAIL, "Found ASCII file.");
        address->file_type = PCCC_FILE_ASCII;
        address->element_size_bytes = 1;
        (*str)++;
        break;

    case 'B':
    case 'b': /* Bit or block transfer */
        if(isdigit((*str)[1])) {
            /* Bit */
            pdebug(DEBUG_DETAIL, "Found Bit file.");
            address->file_type = PCCC_FILE_BIT;
            address->element_size_bytes = 2;
            (*str)++;
            break;
        } else {
            if((*str)[1] == 'T' || (*str)[1] == 't') {
                /* block transfer */
                pdebug(DEBUG_DETAIL, "Found Block Transfer file.");
                address->file_type = PCCC_FILE_BLOCK_TRANSFER;
                address->element_size_bytes = 12;
                (*str) += 2;
            } else {
                pdebug(DEBUG_WARN, "Unknown file %s found!", *str);
                address->file_type = PCCC_FILE_UNKNOWN;
                address->element_size_bytes = 0;
                rc = PLCTAG_ERR_BAD_PARAM;
            }
        }

        break;

    case 'C':
    case 'c': /* Counter */
        pdebug(DEBUG_DETAIL, "Found Counter file.");
        address->file_type = PCCC_FILE_COUNTER;
        address->element_size_bytes = 6;
        (*str)++;
        break;

    case 'D':
    case 'd': /* BCD number */
        pdebug(DEBUG_DETAIL, "Found BCD file.");
        address->file_type = PCCC_FILE_BCD;
        address->element_size_bytes = 2;
        (*str)++;
        break;

    case 'F':
    case 'f': /* Floating point Number */
        pdebug(DEBUG_DETAIL, "Found Float/REAL file.");
        address->file_type = PCCC_FILE_FLOAT;
        address->element_size_bytes = 4;
        (*str)++;
        break;

    case 'I':
    case 'i': /* Input */
        pdebug(DEBUG_DETAIL, "Found Input file.");
        address->file_type = PCCC_FILE_INPUT;
        address->element_size_bytes = 2;
        (*str)++;
        break;

    case 'L':
    case 'l':
        pdebug(DEBUG_DETAIL, "Found Long Int file.");
        address->file_type = PCCC_FILE_LONG_INT;
        address->element_size_bytes = 4;
        (*str)++;
        break;

    case 'M':
    case 'm': /* Message */
        if((*str)[1] == 'G' || (*str)[1] == 'g') {
            pdebug(DEBUG_DETAIL, "Found Message file.");
            address->file_type = PCCC_FILE_MESSAGE;
            address->element_size_bytes = 112;
            (*str) += 2;  /* skip past both characters */
        } else {
            address->file_type = PCCC_FILE_UNKNOWN;
            pdebug(DEBUG_WARN, "Unknown file %s found!", *str);
            rc = PLCTAG_ERR_BAD_PARAM;
        }
        break;

    case 'N':
    case 'n': /* INT */
        pdebug(DEBUG_DETAIL, "Found Integer file.");
        address->file_type = PCCC_FILE_INT;
        address->element_size_bytes = 2;
        (*str)++;
        break;

    case 'O':
    case 'o': /* Output */
        pdebug(DEBUG_DETAIL, "Found Output file.");
        address->file_type = PCCC_FILE_OUTPUT;
        address->element_size_bytes = 2;
        (*str)++;
        break;

    case 'P':
    case 'p': /* PID */
        if((*str)[1] == 'D' || (*str)[1] == 'd') {
            pdebug(DEBUG_DETAIL, "Found PID file.");
            address->file_type = PCCC_FILE_PID;
            address->element_size_bytes = 164;
            (*str) += 2;  /* skip past both characters */
        } else {
            address->file_type = PCCC_FILE_UNKNOWN;
            pdebug(DEBUG_WARN, "Unknown file %s found!", *str);
            rc = PLCTAG_ERR_BAD_PARAM;
        }
        break;

    case 'R':
    case 'r': /* Control */
        pdebug(DEBUG_DETAIL, "Found Control file.");
        address->file_type = PCCC_FILE_CONTROL;
        address->element_size_bytes = 6;
        (*str)++;
        break;

    case 'S':
    case 's': /* Status, SFC or String */
        if(isdigit((*str)[1])) {
            /* Status */
            pdebug(DEBUG_DETAIL, "Found Status file.");
            address->file_type = PCCC_FILE_STATUS;
            address->element_size_bytes = 2;
            (*str)++;
            break;
        } else {
            if((*str)[1] == 'C' || (*str)[1] == 'c') {
                /* SFC */
                pdebug(DEBUG_DETAIL, "Found SFC file.");
                address->file_type = PCCC_FILE_SFC;
                address->element_size_bytes = 6;
                (*str) += 2;  /* skip past both characters */
            } else if((*str)[1] == 'T' || (*str)[1] == 't') {
                /* String */
                pdebug(DEBUG_DETAIL, "Found String file.");
                address->file_type = PCCC_FILE_STRING;
                address->element_size_bytes = 84;
                (*str) += 2;  /* skip past both characters */
            } else {
                address->file_type = PCCC_FILE_UNKNOWN;
                pdebug(DEBUG_WARN, "Unknown file %s found!", *str);
                rc = PLCTAG_ERR_BAD_PARAM;
            }
        }
        break;

    case 'T':
    case 't': /* Timer */
        pdebug(DEBUG_DETAIL, "Found Timer file.");
        address->file_type = PCCC_FILE_TIMER;
        address->element_size_bytes = 6;
        (*str)++;
        break;

    default:
        pdebug(DEBUG_WARN, "Bad format or unsupported logical address %s!", *str);
        address->file_type = PCCC_FILE_UNKNOWN;
        address->element_size_bytes = 0;
        rc = PLCTAG_ERR_BAD_PARAM;
        break;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}



int parse_pccc_file_num(const char **str, pccc_addr_t *address)
{
    int tmp = 0;

    pdebug(DEBUG_DETAIL,"Starting.");

    if(!str || !*str || !isdigit(**str)) {
        pdebug(DEBUG_WARN,"Expected data-table file number!");
        address->file = -1;
        return PLCTAG_ERR_BAD_PARAM;
    }

    while(**str && isdigit(**str) && tmp < 65535) {
        tmp *= 10;
        tmp += (int)((**str) - '0');
        (*str)++;
    }

    address->file = tmp;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}



int parse_pccc_elem_num(const char **str, pccc_addr_t *address)
{
    int tmp = 0;

    pdebug(DEBUG_DETAIL,"Starting.");

    if(!str || !*str || **str != ':') {
        pdebug(DEBUG_WARN,"Expected data-table element number!");
        address->element = -1;
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* step past the : character */
    (*str)++;

    while(**str && isdigit(**str) && tmp < 65535) {
        tmp *= 10;
        tmp += (int)((**str) - '0');
        (*str)++;
    }

    address->element = tmp;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}

struct {
    pccc_file_t file_type;
    const char *field_name;
    int element_size_bytes;
    int sub_element;
    uint8_t is_bit;
    uint8_t bit;
} sub_element_lookup[] = {
    /* file type                    field   size    subelem is_bit  bit_num */

    /* BT block transfer */
    { PCCC_FILE_BLOCK_TRANSFER,     "con",  2,      0,      0,      0},
    { PCCC_FILE_BLOCK_TRANSFER,     "rlen", 2,      1,      0,      0},
    { PCCC_FILE_BLOCK_TRANSFER,     "dlen", 2,      2,      0,      0},
    { PCCC_FILE_BLOCK_TRANSFER,     "df",   2,      3,      0,      0},
    { PCCC_FILE_BLOCK_TRANSFER,     "elem", 2,      4,      0,      0},
    { PCCC_FILE_BLOCK_TRANSFER,     "rgs",  2,      5,      0,      0},

    /* R Control */
    { PCCC_FILE_CONTROL,            "con",  2,      0,      0,      0},
    { PCCC_FILE_CONTROL,            "len",  2,      1,      0,      0},
    { PCCC_FILE_CONTROL,            "pos",  2,      2,      0,      0},

    /* C Counter */
    { PCCC_FILE_COUNTER,            "con",  2,      0,      0,      0},
    { PCCC_FILE_COUNTER,            "cu",   2,      0,      1,      15},
    { PCCC_FILE_COUNTER,            "cd",   2,      0,      1,      14},
    { PCCC_FILE_COUNTER,            "dn",   2,      0,      1,      13},
    { PCCC_FILE_COUNTER,            "ov",   2,      0,      1,      12},
    { PCCC_FILE_COUNTER,            "un",   2,      0,      1,      11},
    { PCCC_FILE_COUNTER,            "pre",  2,      1,      0,      0},
    { PCCC_FILE_COUNTER,            "acc",  2,      2,      0,      0},

    /* MG Message */
    { PCCC_FILE_MESSAGE,            "con",  2,      0,      0,      0},
    { PCCC_FILE_MESSAGE,            "nr",   2,      0,      1,      9},
    { PCCC_FILE_MESSAGE,            "to",   2,      0,      1,      8},
    { PCCC_FILE_MESSAGE,            "en",   2,      0,      1,      7},
    { PCCC_FILE_MESSAGE,            "st",   2,      0,      1,      6},
    { PCCC_FILE_MESSAGE,            "dn",   2,      0,      1,      5},
    { PCCC_FILE_MESSAGE,            "er",   2,      0,      1,      4},
    { PCCC_FILE_MESSAGE,            "co",   2,      0,      1,      3},
    { PCCC_FILE_MESSAGE,            "ew",   2,      0,      1,      2},
    { PCCC_FILE_MESSAGE,            "err",  2,      1,      0,      0},
    { PCCC_FILE_MESSAGE,            "rlen", 2,      2,      0,      0},
    { PCCC_FILE_MESSAGE,            "dlen", 2,      3,      0,      0},
    { PCCC_FILE_MESSAGE,            "data", 104,    4,      0,      0},

    /* PID */
    /* PD first control word */
    { PCCC_FILE_PID,                "con",  2,      0,      0,      0},
    { PCCC_FILE_PID,                "en",   2,      0,      1,      15},
    { PCCC_FILE_PID,                "ct",   2,      0,      1,      9},
    { PCCC_FILE_PID,                "cl",   2,      0,      1,      8},
    { PCCC_FILE_PID,                "pvt",  2,      0,      1,      7},
    { PCCC_FILE_PID,                "do",   2,      0,      1,      6},
    { PCCC_FILE_PID,                "swm",  2,      0,      1,      4},
    { PCCC_FILE_PID,                "do",   2,      0,      1,      2},
    { PCCC_FILE_PID,                "mo",   2,      0,      1,      1},
    { PCCC_FILE_PID,                "pe",   2,      0,      1,      0},

    /* second control word */
    { PCCC_FILE_PID,                "ini",  2,      1,      1,      12},
    { PCCC_FILE_PID,                "spor", 2,      1,      1,      11},
    { PCCC_FILE_PID,                "oll",  2,      1,      1,      10},
    { PCCC_FILE_PID,                "olh",  2,      1,      1,      9},
    { PCCC_FILE_PID,                "ewd",  2,      1,      1,      8},
    { PCCC_FILE_PID,                "dvna", 2,      1,      1,      3},
    { PCCC_FILE_PID,                "dvpa", 2,      1,      1,      2},
    { PCCC_FILE_PID,                "pvla", 2,      1,      1,      1},
    { PCCC_FILE_PID,                "pvha", 2,      1,      1,      0},

    /* main PID vars */
    { PCCC_FILE_PID,                "sp",   4,      2,      0,      0},
    { PCCC_FILE_PID,                "kp",   4,      4,      0,      0},
    { PCCC_FILE_PID,                "ki",   4,      6,      0,      0},
    { PCCC_FILE_PID,                "kd",   4,      8,      0,      0},

    { PCCC_FILE_PID,                "bias", 4,      10,     0,      0},
    { PCCC_FILE_PID,                "maxs", 4,      12,     0,      0},
    { PCCC_FILE_PID,                "mins", 4,      14,     0,      0},
    { PCCC_FILE_PID,                "db",   4,      16,     0,      0},
    { PCCC_FILE_PID,                "so",   4,      18,     0,      0},
    { PCCC_FILE_PID,                "maxo", 4,      20,     0,      0},
    { PCCC_FILE_PID,                "mino", 4,      22,     0,      0},
    { PCCC_FILE_PID,                "upd",  4,      24,     0,      0},

    { PCCC_FILE_PID,                "pv",   4,      26,     0,      0},

    { PCCC_FILE_PID,                "err",  4,      28,     0,      0},
    { PCCC_FILE_PID,                "out",  4,      30,     0,      0},
    { PCCC_FILE_PID,                "pvh",  4,      32,     0,      0},
    { PCCC_FILE_PID,                "pvl",  4,      34,     0,      0},
    { PCCC_FILE_PID,                "dvp",  4,      36,     0,      0},
    { PCCC_FILE_PID,                "dvn",  4,      38,     0,      0},

    { PCCC_FILE_PID,                "pvdb", 4,      40,     0,      0},
    { PCCC_FILE_PID,                "dvdb", 4,      42,     0,      0},
    { PCCC_FILE_PID,                "maxi", 4,      44,     0,      0},
    { PCCC_FILE_PID,                "mini", 4,      46,     0,      0},
    { PCCC_FILE_PID,                "tie",  4,      48,     0,      0},

    { PCCC_FILE_PID,                "addr", 8,      48,     0,      0},

    { PCCC_FILE_PID,                "data", 56,     52,     0,      0},

    /* ST String */
    { PCCC_FILE_STRING,             "len",  2,      0,      0,      0},
    { PCCC_FILE_STRING,             "data", 82,     1,      0,      0},

    /* SC SFC */
    { PCCC_FILE_SFC,                "con",  2,      0,      0,      0},
    { PCCC_FILE_SFC,                "sa",   2,      0,      1,      15},
    { PCCC_FILE_SFC,                "fs",   2,      0,      1,      14},
    { PCCC_FILE_SFC,                "ls",   2,      0,      1,      13},
    { PCCC_FILE_SFC,                "ov",   2,      0,      1,      12},
    { PCCC_FILE_SFC,                "er",   2,      0,      1,      11},
    { PCCC_FILE_SFC,                "dn",   2,      0,      1,      10},
    { PCCC_FILE_SFC,                "pre",  2,      1,      0,      0},
    { PCCC_FILE_SFC,                "tim",  2,      2,      0,      0},

    /* T timer */
    { PCCC_FILE_TIMER,              "con",  2,      0,      0,      0},
    { PCCC_FILE_TIMER,              "en",   2,      0,      1,      15},
    { PCCC_FILE_TIMER,              "tt",   2,      0,      1,      14},
    { PCCC_FILE_TIMER,              "dn",   2,      0,      1,      13},
    { PCCC_FILE_TIMER,              "pre",  2,      1,      0,      0},
    { PCCC_FILE_TIMER,              "acc",  2,      2,      0,      0}
};


int parse_pccc_subelem_mnemonic(const char **str, pccc_addr_t *address)
{
    pdebug(DEBUG_DETAIL,"Starting.");

    if(!str || !*str) {
        pdebug(DEBUG_WARN,"Called with bad string pointer!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /*
     * if we have a null character we are at the end of the name
     * and the subelement is not there.  That is not an error.
     */

    if( (**str) == 0) {
        pdebug(DEBUG_DETAIL, "No subelement in this name.");
        address->sub_element = -1;
        return PLCTAG_STATUS_OK;
    }

    /*
     * We do have a character.  It must be . or / to be valid.
     * The . character is valid before a mnemonic for a field in a structured type.
     * The / character is valid before a bit number.
     *
     * If we see a bit number, then punt out of this routine.
     */

    if((**str) == '/') {
        pdebug(DEBUG_DETAIL, "No subelement in this logical address.");
        address->sub_element = -1;
        return PLCTAG_STATUS_OK;
    }

    /* make sure the next character is either / or . and nothing else. */
    if((**str) != '.') {
        pdebug(DEBUG_WARN, "Bad subelement field in logical address.");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* step past the . character */
    (*str)++;

    /* search for a match. */
    for(size_t i=0; i < (sizeof(sub_element_lookup)/sizeof(sub_element_lookup[0])); i++) {
        if(sub_element_lookup[i].file_type == address->file_type && str_cmp_i_n(*str, sub_element_lookup[i].field_name, str_length(sub_element_lookup[i].field_name)) == 0) {
            pdebug(DEBUG_DETAIL, "Matched file type %x and field mnemonic \"%.*s\".", address->file_type, str_length(sub_element_lookup[i].field_name), *str);

            address->is_bit = sub_element_lookup[i].is_bit;
            address->bit = sub_element_lookup[i].bit;
            address->sub_element = sub_element_lookup[i].sub_element;
            address->element_size_bytes = sub_element_lookup[i].element_size_bytes;

            /* bump past the field name */
            (*str) += str_length(sub_element_lookup[i].field_name);

            return PLCTAG_STATUS_OK;
        }
    }

    pdebug(DEBUG_WARN, "Unsupported field mnemonic %s for type %x!", *str, address->file_type);

    return PLCTAG_ERR_BAD_PARAM;
}



int parse_pccc_bit_num(const char **str, pccc_addr_t *address)
{
    int tmp = 0;

    pdebug(DEBUG_DETAIL,"Starting.");

    if(!str || !*str) {
        pdebug(DEBUG_WARN,"Called with bad string pointer!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /*
     * if we have a null character we are at the end of the name
     * and the subelement is not there.  That is not an error.
     */

    if( (**str) == 0) {
        pdebug(DEBUG_DETAIL, "No bit number in this name.");
        return PLCTAG_STATUS_OK;
    }

    /* make sure the next character is /. */
    if((**str) != '/') {
        pdebug(DEBUG_WARN, "Bad bit number in logical address.");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* make sure that the data size is two bytes only. */
    if(address->element_size_bytes != 2) {
        pdebug(DEBUG_WARN, "Single bit selection only works on word-sized data.");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* step past the / character */
    (*str)++;

    /* FIXME - we do this a lot, should be a small routine. */
    while(**str && isdigit(**str) && tmp < 65535) {
        tmp *= 10;
        tmp += (int)((**str) - '0');
        (*str)++;
    }

    if(tmp < 0 || tmp > 15) {
        pdebug(DEBUG_WARN, "Error processing bit number.  Must be between 0 and 15 inclusive, found %d!", tmp);
        return PLCTAG_ERR_OUT_OF_BOUNDS;
    }

    address->is_bit = (uint8_t)1;
    address->bit = (uint8_t)tmp;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}


void encode_data(uint8_t *data, int *index, int val)
{
    if(val <= 254) {
        data[*index] = (uint8_t)val;
        *index = *index + 1;
    } else {
        data[*index] = (uint8_t)0xff;
        data[*index + 1] = (uint8_t)(val & 0xff);
        data[*index + 2] = (uint8_t)((val >> 8) & 0xff);
        *index = *index + 3;
    }
}


// int encode_file_type(pccc_file_t file_type)
// {
//     switch(file_type) {
//         case PCCC_FILE_ASCII: return 0x8e; break;
//         case PCCC_FILE_BIT: return 0x85; break;
//         case PCCC_FILE_BLOCK_TRANSFER: break;
//         case PCCC_FILE_COUNTER: return 0x87; break;
//         case PCCC_FILE_BCD: return 0x8f; break;
//         case PCCC_FILE_FLOAT: return 0x8a; break;
//         case PCCC_FILE_INPUT: return 0x8c; break;
//         case PCCC_FILE_LONG_INT: return 0x91; break;
//         case PCCC_FILE_MESSAGE: return 0x92; break;
//         case PCCC_FILE_INT: return 0x89; break;
//         case PCCC_FILE_OUTPUT: return 0x8b; break;
//         case PCCC_FILE_PID: return 0x93; break;
//         case PCCC_FILE_CONTROL: return 0x88; break;
//         case PCCC_FILE_STATUS: return 0x84; break;
//         case PCCC_FILE_SFC: break; /* what is this? */
//         case PCCC_FILE_STRING: return 0x8d; break;
//         case PCCC_FILE_TIMER: return 0x86; break;




//         default:
//             pdebug(DEBUG_WARN, "Unknown file type %d!", (int)file_type);
//             return 0x00;
//             break;
//     }

//     pdebug(DEBUG_WARN, "Unknown file type %d!", (int)file_type);
//     return 0x00;
// }

#endif // __PROTOCOLS_AB_PCCC_C__


#ifndef __PROTOCOLS_AB_ERROR_CODES_C__
#define __PROTOCOLS_AB_ERROR_CODES_C__

struct error_code_entry {
    int primary_code;
    int secondary_code;
    int translated_code;
    const char *short_desc;
    const char *long_desc;
};


/*
 * This information was constructed after finding a few online resources.  Most of it comes from publically published manuals for other products.
 * Sources include:
 * 	Kepware
 *  aboutplcs.com (Productivity 3000 manual)
 *  Allen-Bradley
 *  and others I have long since lost track of.
 *
 * Most probably comes from aboutplcs.com.
 *
 * The copyright on these entries that of their respective owners.  Used here under assumption of Fair Use.
 */


static struct error_code_entry error_code_table[] = {
    {0x01, 0x0100, PLCTAG_ERR_DUPLICATE, "Connection In Use/Duplicate Forward Open", "A connection is already established from the target device sending a Forward Open request or the target device has sent multiple forward open request. This could be caused by poor network traffic. Check the cabling, switches and connections."},
    {0x01, 0x0103, PLCTAG_ERR_UNSUPPORTED, "Transport Class/Trigger Combination not supported", "The Transport class and trigger combination is not supported. The Productivity Suite CPU only supports Class 1 and Class 3 transports and triggers: Change of State and Cyclic."},
    {0x01, 0x0106, PLCTAG_ERR_NOT_ALLOWED, "Owner Conflict", "An existing exclusive owner has already configured a connection to this Connection Point. Check to see if other Scanner devices are connected to this adapter or verify that Multicast is supported by adapter device if Multicast is selected for Forward Open. This could be caused by poor network traffic. Check the cabling, switches and connections."},
    {0x01, 0x0107, PLCTAG_ERR_NOT_FOUND, "Target Connection Not Found", "This occurs if a device sends a Forward Close on a connection and the device can't find this connection. This could occur if one of these devices has powered down or if the connection timed out on a bad connection. This could be caused by poor network traffic. Check the cabling, switches and connections."},
    {0x01, 0x0108, PLCTAG_ERR_BAD_PARAM, "Invalid Network Connection Parameter", "This error occurs when one of the parameters specified in the Forward Open message is not supported such as Connection Point, Connection type, Connection priority, redundant owner or exclusive owner. The Productivity Suite CPU does not return this error and will instead use errors 0x0120, 0x0121, 0x0122, 0x0123, 0x0124, 0x0125 or 0x0132 instead."},
    {0x01, 0x0109, PLCTAG_ERR_BAD_PARAM, "Invalid Connection Size", "This error occurs when the target device doesn't support the requested connection size. Check the documentation of the manufacturer's device to verify the correct Connection size required by the device. Note that most devices specify this value in terms of bytes. The Productivity Suite CPU does not return this error and will instead use errors 0x0126, 0x0127 and 0x0128."},
    {0x01, 0x0110, PLCTAG_ERR_NOT_FOUND, "Target for Connection Not Configured", "This error occurs when a message is received with a connection number that does not exist in the target device. This could occur if the target device has powered down or if the connection timed out. This could be caused by poor network traffic. Check the cabling, switches and connections."},
    {0x01, 0x0111, PLCTAG_ERR_UNSUPPORTED, "RPI Not Supported", "This error occurs if the Originator is specifying an RPI that is not supported. The Productivity Suite CPU will accept a minimum value of 10ms on a CIP Forward Open request. However, the CPU will produce at the specified rate up to the scan time of the installed project. The CPU cannot product any faster than the scan time of the running project."},
    {0x01, 0x0112, PLCTAG_ERR_BAD_PARAM, "RPI Value not acceptable", "This error can be returned if the Originator is specifying an RPI value that is not acceptable. There may be six additional values following the extended error code with the acceptable values. An array can be defined for this field in order to view the extended error code attributes. If the Target device supports extended status, the format of the values will be as shown below:\nUnsigned Integer 16, Value = 0x0112, Explanation: Extended Status code,\nUnsigned Integer 8, Value = variable, Explanation: Acceptable Originator to Target RPI type, values: 0 = The RPI specified in the forward open was acceptable (O -> T value is ignored), 1 = unspecified (use a different RPI), 2 = minimum acceptable RPI (too fast), 3 = maximum acceptable RPI (too slow), 4 = required RPI to corrected mismatch (data is already being consumed at a different RPI), 5 to 255 = reserved.\nUnsigned Integer 32, Value = variable, Explanation: Value of O -> T RPI that is within the acceptable range for the application.\nUnsigned Integer 32, Value = variable, Explanation: Value of T -> O RPI that is within the acceptable range for the application."},
    {0x01, 0x0113, PLCTAG_ERR_NO_RESOURCES, "Out of Connections", "The Productivity Suite EtherNet/IP Adapter connection limit of 4 when doing Class 3 connections has been reached. An existing connection must be dropped in order for a new one to be generated."},
    {0x01, 0x0114, PLCTAG_ERR_NOT_FOUND, "Vendor ID or Product Code Mismatch", "The compatibility bit was set in the Forward Open message but the Vendor ID or Product Code did not match."},
    {0x01, 0x0115, PLCTAG_ERR_NOT_FOUND, "Device Type Mismatch", "The compatibility bit was set in the Forward Open message but the Device Type did not match."},
    {0x01, 0x0116, PLCTAG_ERR_NO_MATCH, "Revision Mismatch", "The compatibility bit was set in the Forward Open message but the major and minor revision numbers were not a valid revision."},
    {0x01, 0x0117, PLCTAG_ERR_BAD_PARAM, "Invalid Produced or Consumed Application Path", "This error is returned from the Target device when the Connection Point parameters specified for the O -> T (Output) or T -> O (Input) connection is incorrect or not supported. The Productivity Suite CPU does not return this error and uses the following error codes instead: 0x012A, 0x012B or 0x012F."},
    {0x01, 0x0118, PLCTAG_ERR_BAD_PARAM, "Invalid or Inconsistent Configuration Application Path", "This error is returned from the Target device when the Connection Point parameter specified for the Configuration data is incorrect or not supported. The Productivity Suite CPU does not return this error and uses the following error codes instead: 0x0129 or 0x012F."},
    {0x01, 0x0119, PLCTAG_ERR_OPEN, "Non-listen Only Connection Not Opened", "This error code is returned when an Originator device attempts to establish a listen only connection and there is no non-listen only connection established. The Productivity Suite CPU does not support listen only connections as Scanner or Adapter."},
    {0x01, 0x011A, PLCTAG_ERR_NO_RESOURCES, "Target Object Out of Connections", "The maximum number of connections supported by this instance of the object has been exceeded."},
    {0x01, 0x011B, PLCTAG_ERR_TOO_SMALL, "RPI is smaller than the Production Inhibit Time", "The Target to Originator RPI is smaller than the Target to Originator Production Inhibit Time. Consult the manufacturer's documentation as to the minimum rate that data can be produced and adjust the RPI to greater than this value."},
    {0x01, 0x011C, PLCTAG_ERR_UNSUPPORTED, "Transport Class Not Supported", "The Transport Class requested in the Forward Open is not supported. Only Class 1 and Class 3 classes are supported in the Productivity Suite CPU."},
    {0x01, 0x011D, PLCTAG_ERR_UNSUPPORTED, "Production Trigger Not Supported", "The Production Trigger requested in the Forward Open is not supported. In Class 1, only Cyclic and Change of state are supported in the Productivity Suite CPU. In Class 3, Application object is supported."},
    {0x01, 0x011E, PLCTAG_ERR_UNSUPPORTED, "Direction Not Supported", "The Direction requested in the Forward Open is not supported."},
    {0x01, 0x011F, PLCTAG_ERR_BAD_PARAM, "Invalid Originator to Target Network Connection Fixed/Variable Flag", "The Originator to Target fixed/variable flag specified in the Forward Open is not supported . Only Fixed is supported in the Productivity Suite CPU."},
    {0x01, 0x0120, PLCTAG_ERR_BAD_PARAM, "Invalid Target to Originator Network Connection Fixed/Variable Flag", "The Target to Originator fixed/variable flag specified in the Forward Open is not supported. Only Fixed is supported in the Productivity Suite CPU."},
    {0x01, 0x0121, PLCTAG_ERR_BAD_PARAM, "Invalid Originator to Target Network Connection Priority", "The Originator to Target Network Connection Priority specified in the Forward Open is not supported. Low, High, Scheduled and Urgent are supported in the Productivity Suite CPU."},
    {0x01, 0x0122, PLCTAG_ERR_BAD_PARAM, "Invalid Target to Originator Network Connection Priority", "The Target to Originator Network Connection Priority specified in the Forward Open is not supported. Low, High, Scheduled and Urgent are supported in the Productivity Suite CPU."},
    {0x01, 0x0123, PLCTAG_ERR_BAD_PARAM, "Invalid Originator to Target Network Connection Type", "The Originator to Target Network Connection Type specified in the Forward Open is not supported. Only Unicast is supported for O -> T (Output) data in the Productivity Suite CPU."},
    {0x01, 0x0124, PLCTAG_ERR_BAD_PARAM, "Invalid Target to Originator Network Connection Type", "The Target to Originator Network Connection Type specified in the Forward Open is not supported. Multicast and Unicast is supported in the Productivity Suite CPU. Some devices may not support one or the other so if this error is encountered try the other method."},
    {0x01, 0x0125, PLCTAG_ERR_BAD_PARAM, "Invalid Originator to Target Network Connection Redundant_Owner", "The Originator to Target Network Connection Redundant_Owner flag specified in the Forward Open is not supported. Only Exclusive owner connections are supported in the Productivity Suite CPU."},
    {0x01, 0x0126, PLCTAG_ERR_BAD_PARAM, "Invalid Configuration Size", "This error is returned when the Configuration data sent in the Forward Open does not match the size specified or is not supported by the Adapter. The Target device may return an additional Unsigned Integer 16 value that specifies the maximum size allowed for this data. An array can be defined for this field in order to view the extended error code attributes."},
    {0x01, 0x0127, PLCTAG_ERR_BAD_PARAM, "Invalid Originator to Target Size", "This error is returned when the Originator to Target (Output data) size specified in the Forward Open does not match what is in the Target. Consult the documentation of the Adapter device to verify the required size. Note that if the Run/Idle header is requested, it will add 4 additional bytes and must be accounted for in the Forward Open calculation. The Productivity Suite CPU always requires the Run/Idle header so if the option doesn't exist in the Scanner device, you must add an additional 4 bytes to the O -> T (Output) setup. Some devices may publish the size that they are looking for as an additional attribute (Unsigned Integer 16 value) of the Extended Error Code. An array can be defined for this field in order to view the extended error code attributes.\nNote: This error may also be generated when a Connection Point value that is invalid for IO Messaging (but valid for other cases such as Explicit Messaging) is specified, such as 0. Please verify if the Connection Point value is valid for IO Messaging in the target device."},
    {0x01, 0x0128, PLCTAG_ERR_BAD_PARAM, "Invalid Target to Originator Size", "This error is returned when the Target to Originator (Input data) size specified in the Forward Open does not match what is in Target. Consult the documentation of the Adapter device to verify the required size. Note that if the Run/Idle header is requested, it will add 4 additional bytes and must be accounted for in the Forward Open calculation. The Productivity Suite CPU does not support a Run/Idle header for the T -> O (Input) data. Some devices may publish the size that they are looking for as an additional attribute (Unsigned Integer 16 value) of the Extended Error Code. An array can be defined for this field in order to view the extended error code attributes.\nNote: This error may also be generated when a Connection Point value that is invalid for IO Messaging (but valid for other cases such as Explicit Messaging) is specified, such as 0. Please verify if the Connection Point value is valid for IO Messaging in the target device."},
    {0x01, 0x0129, PLCTAG_ERR_BAD_PARAM, "Invalid Configuration Application Path", "This error will be returned by the Productivity Suite CPU if a Configuration Connection with a size other than 0 is sent to the CPU. The Configuration Connection size must always be zero if it this path is present in the Forward Open message coming from the Scanner device."},
    {0x01, 0x012A, PLCTAG_ERR_BAD_PARAM, "Invalid Consuming Application Path", "This error will be returned by the Productivity Suite CPU if the Consuming (O -> T) Application Path is not present in the Forward Open message coming from the Scanner device or if the specified Connection Point is incorrect."},
    {0x01, 0x012B, PLCTAG_ERR_BAD_PARAM, "Invalid Producing Application Path", "This error will be returned by the Productivity Suite CPU if the Producing (T -> O) Application Path is not present in the Forward Open message coming from the Scanner device or if the specified Connection Point is incorrect."},
    {0x01, 0x012C, PLCTAG_ERR_NOT_FOUND, "Configuration Symbol Does not Exist", "The Originator attempted to connect to a configuration tag name that is not supported in the Target."},
    {0x01, 0x012D, PLCTAG_ERR_NOT_FOUND, "Consuming Symbol Does not Exist", "The Originator attempted to connect to a consuming tag name that is not supported in the Target."},
    {0x01, 0x012E, PLCTAG_ERR_NOT_FOUND, "Producing Symbol Does not Exist", "The Originator attempted to connect to a producing tag name that is not supported in the Target."},
    {0x01, 0x012F, PLCTAG_ERR_BAD_DATA, "Inconsistent Application Path Combination", "The combination of Configuration, Consuming and Producing application paths specified are inconsistent."},
    {0x01, 0x0130, PLCTAG_ERR_BAD_DATA, "Inconsistent Consume data format", "Information in the data segment not consistent with the format of the data in the consumed data."},
    {0x01, 0x0131, PLCTAG_ERR_BAD_DATA, "Inconsistent Product data format", "Information in the data segment not consistent with the format of the data in the produced data."},
    {0x01, 0x0132, PLCTAG_ERR_UNSUPPORTED, "Null Forward Open function not supported", "The target device does not support the function requested in the NULL Forward Open request. The request could be such items as Ping device, Configure device application, etc."},
    {0x01, 0x0133, PLCTAG_ERR_BAD_PARAM, "Connection Timeout Multiplier not acceptable", "The Connection Multiplier specified in the Forward Open request not acceptable by the Target device (once multiplied in conjunction with the specified timeout value). Consult the manufacturer device's documentation on what the acceptable timeout and multiplier are for this device."},
    {0x01, 0x0203, PLCTAG_ERR_TIMEOUT, "Connection Timed Out", "This error will be returned by the Productivity Suite CPU if a message is sent to the CPU on a connection that has already timed out. Connections time out if no message is sent to the CPU in the time period specified by the RPI rate X Connection multiplier specified in the Forward Open message."},
    {0x01, 0x0204, PLCTAG_ERR_TIMEOUT, "Unconnected Request Timed Out", "This time out occurs when the device sends an Unconnected Request and no response is received within the specified time out period. In the Productivity Suite CPU, this value may be found in the hardware configuration under the Ethernet port settings for the P3-550 or P3-530."},
    {0x01, 0x0205, PLCTAG_ERR_BAD_PARAM, "Parameter Error in Unconnected Request Service", "This error occurs when Connection Tick Time/Connection time-out combination is specified in the Forward Open or Forward Close message is not supported by the device."},
    {0x01, 0x0206, PLCTAG_ERR_TOO_LARGE, "Message Too Large for Unconnected_Send Service", "Occurs when Unconnected_Send message is too large to be sent to the network."},
    {0x01, 0x0207, PLCTAG_ERR_BAD_REPLY, "Unconnected Acknowledge without Reply", "This error occurs if an Acknowledge was received but no data response occurred. Verify that the message that was sent is supported by the Target device using the device manufacturer's documentation."},
    {0x01, 0x0301, PLCTAG_ERR_NO_MEM, "No Buffer Memory Available", "This error occurs if the Connection memory buffer in the target device is full. Correct this by reducing the frequency of the messages being sent to the device and/or reducing the number of connections to the device. Consult the manufacturer's documentation for other means of correcting this."},
    {0x01, 0x0302, PLCTAG_ERR_NO_RESOURCES, "Network Bandwidth not Available for Data", "This error occurs if the Producer device cannot support the specified RPI rate when the connection has been configured with schedule priority. Reduce the RPI rate or consult the manufacturer's documentation for other means to correct this."},
    {0x01, 0x0303, PLCTAG_ERR_NO_RESOURCES, "No Consumed Connection ID Filter Available", "This error occurs if a Consumer device doesn't have an available consumed_connection_id filter."},
    {0x01, 0x0304, PLCTAG_ERR_BAD_CONFIG, "Not Configured to Send Scheduled Priority Data", "This error occurs if a device has been configured for a scheduled priority message and it cannot send the data at the scheduled time slot."},
    {0x01, 0x0305, PLCTAG_ERR_NO_MATCH, "Schedule Signature Mismatch", "This error occurs if the schedule priority information does not match between the Target and the Originator."},
    {0x01, 0x0306, PLCTAG_ERR_UNSUPPORTED, "Schedule Signature Validation not Possible", "This error occurs when the schedule priority information sent to the device is not validated."},
    {0x01, 0x0311, PLCTAG_ERR_BAD_DEVICE, "Port Not Available", "This error occurs when a port number specified in a port segment is not available. Consult the documentation of the device to verify the correct port number."},
    {0x01, 0x0312, PLCTAG_ERR_BAD_PARAM, "Link Address Not Valid", "The Link address specified in the port segment is not correct. Consult the documentation of the device to verify the correct port number."},
    {0x01, 0x0315, PLCTAG_ERR_BAD_PARAM, "Invalid Segment in Connection Path", "This error occurs when the target device cannot understand the segment type or segment value in the Connection Path. Consult the documentation of the device to verify the correct segment type and value. If a Connection Point greater than 255 is specified this error could occur."},
    {0x01, 0x0316, PLCTAG_ERR_NO_MATCH, "Forward Close Service Connection Path Mismatch", "This error occurs when the Connection path in the Forward Close message does not match the Connection Path configured in the connection. Contact Tech Support if this error persists."},
    {0x01, 0x0317, PLCTAG_ERR_BAD_PARAM, "Scheduling Not Specified", "This error can occur if the Schedule network segment or value is invalid."},
    {0x01, 0x0318, PLCTAG_ERR_BAD_PARAM, "Link Address to Self Invalid", "If the Link address points back to the originator device, this error will occur."},
    {0x01, 0x0319, PLCTAG_ERR_NO_RESOURCES, "Secondary Resource Unavailable", "This occurs in a redundant system when the secondary connection request is unable to duplicate the primary connection request."},
    {0x01, 0x031A, PLCTAG_ERR_DUPLICATE, "Rack Connection Already established", "The connection to a module is refused because part or all of the data requested is already part of an existing rack connection."},
    {0x01, 0x031B, PLCTAG_ERR_DUPLICATE, "Module Connection Already established", "The connection to a rack is refused because part or all of the data requested is already part of an existing module connection."},
    {0x01, 0x031C, PLCTAG_ERR_REMOTE_ERR, "Miscellaneous", "This error is returned when there is no other applicable code for the error condition. Consult the manufacturer's documentation or contact Tech support if this error persist."},
    {0x01, 0x031D, PLCTAG_ERR_NO_MATCH, "Redundant Connection Mismatch", "This error occurs when these parameters don't match when establishing a redundant owner connection: O -> T RPI, O -> T Connection Parameters, T -> O RPI, T -> O Connection Parameters and Transport Type and Trigger."},
    {0x01, 0x031E, PLCTAG_ERR_NO_RESOURCES, "No more User Configurable Link Resources Available in the Producing Module", "This error is returned from the Target device when no more available Consumer connections available for a Producer."},
    {0x01, 0x031F, PLCTAG_ERR_NO_RESOURCES, "No User Configurable Link Consumer Resources Configured in the Producing Module", "This error is returned from the Target device when no Consumer connections have been configured for a Producer connection."},
    {0x01, 0x0800, PLCTAG_ERR_BAD_DEVICE, "Network Link Offline", "The Link path is invalid or not available."},
    {0x01, 0x0810, PLCTAG_ERR_NO_DATA, "No Target Application Data Available", "This error is returned from the Target device when the application has no valid data to produce."},
    {0x01, 0x0811, PLCTAG_ERR_NO_DATA, "No Originator Application Data Available", "This error is returned from the Originator device when the application has no valid data to produce."},
    {0x01, 0x0812, PLCTAG_ERR_UNSUPPORTED, "Node Address has changed since the Network was scheduled", "This specifies that the router has changed node addresses since the value configured in the original connection."},
    {0x01, 0x0813, PLCTAG_ERR_UNSUPPORTED, "Not Configured for Off-subnet Multicast", "The producer has been requested to support a Multicast connection for a consumer on a different subnet and does not support this functionality."},
    {0x01, 0x0814, PLCTAG_ERR_BAD_DATA, "Invalid Produce/Consume Data format", "Information in the data segment not consistent with the format of the data in the consumed or produced data. Errors 0x0130 and 0x0131 are typically used for this situation in most devices now."},
    {0x02, -1, PLCTAG_ERR_NO_RESOURCES, "Resource Unavailable for Unconnected Send", "The Target device does not have the resources to process the Unconnected Send request."},
    {0x03, -1, PLCTAG_ERR_BAD_PARAM, "Parameter value invalid.", ""},
    {0x04, -1, PLCTAG_ERR_NOT_FOUND,"IOI could not be deciphered or tag does not exist.", "The path segment identifier or the segment syntax was not understood by the target device."},
    {0x05, -1, PLCTAG_ERR_BAD_PARAM, "Path Destination Error", "The Class, Instance or Attribute value specified in the Unconnected Explicit Message request is incorrect or not supported in the Target device. Check the manufacturer's documentation for the correct codes to use."},
    {0x06, -1, PLCTAG_ERR_TOO_LARGE, "Data requested would not fit in response packet.", "The data to be read/written needs to be broken up into multiple packets.0x070000 Connection lost: The messaging connection was lost."},
    {0x07, -1, PLCTAG_ERR_BAD_CONNECTION, "Connection lost", "The messaging connection was lost."},
    {0x08, -1, PLCTAG_ERR_UNSUPPORTED, "Unsupported service.", ""},
    {0x09, -1, PLCTAG_ERR_BAD_DATA, "Error in Data Segment", "This error code is returned when an error is encountered in the Data segment portion of a Forward Open message. The Extended Status value is the offset in the Data segment where the error was encountered."},
    {0x0A, -1, PLCTAG_ERR_BAD_STATUS, "Attribute list error", "An attribute in the Get_Attribute_List or Set_Attribute_List response has a non-zero status."},
    {0x0B, -1, PLCTAG_ERR_DUPLICATE, "Already in requested mode/state", "The object is already in the mode/state being requested by the service."},
    {0x0C, -1, PLCTAG_ERR_BAD_STATUS, "Object State Error", "This error is returned from the Target device when the current state of the Object requested does not allow it to be returned. The current state can be specified in the Optional Extended Error status field."},
    {0x0D, -1, PLCTAG_ERR_DUPLICATE, "Object already exists.", "The requested instance of object to be created already exists."},
    {0x0E, -1, PLCTAG_ERR_NOT_ALLOWED, "Attribute not settable", "A request to modify non-modifiable attribute was received."},
    {0x0F, -1, PLCTAG_ERR_NOT_ALLOWED, "Permission denied.", ""},
    {0x10, -1, PLCTAG_ERR_BAD_STATUS, "Device State Error", "This error is returned from the Target device when the current state of the Device requested does not allow it to be returned. The current state can be specified in the Optional Extended Error status field. Check your configured connections points for other Client devices using this same connection."},
    {0x11, -1, PLCTAG_ERR_TOO_LARGE, "Reply data too large", "The data to be transmitted in the response buffer is larger than the allocated response buffer."},
    {0x12, -1, PLCTAG_ERR_NOT_ALLOWED, "Fragmentation of a primitive value", "The service specified an operation that is going to fragment a primitive data value. For example, trying to send a 2 byte value to a REAL data type (4 byte)."},
    {0x13, -1, PLCTAG_ERR_TOO_SMALL, "Not Enough Data", "Not enough data was supplied in the service request specified."},
    {0x14, -1, PLCTAG_ERR_UNSUPPORTED, "Attribute not supported.", "The attribute specified in the request is not supported."},
    {0x15, -1, PLCTAG_ERR_TOO_LARGE, "Too Much Data", "Too much data was supplied in the service request specified."},
    {0x16, -1, PLCTAG_ERR_NOT_FOUND, "Object does not exist.", "The object specified does not exist in the device."},
    {0x17, -1, PLCTAG_ERR_NOT_ALLOWED, "Service fragmentation sequence not in progress.", "The fragmentation sequence for this service is not currently active for this data."},
    {0x18, -1, PLCTAG_ERR_NO_DATA, "No stored attribute data.", "The attribute data of this object was not saved prior to the requested service."},
    {0x19, -1, PLCTAG_ERR_REMOTE_ERR, "Store operation failure.", "The attribute data of this object was not saved due to a failure during the attempt."},
    {0x1A, -1, PLCTAG_ERR_TOO_LARGE, "Routing failure, request packet too large.", "The service request packet was too large for transmission on a network in the path to the destination."},
    {0x1B, -1, PLCTAG_ERR_TOO_LARGE, "Routing failure, response packet too large.", "The service reponse packet was too large for transmission on a network in the path from the destination."},
    {0x1C, -1, PLCTAG_ERR_NO_DATA, "Missing attribute list entry data.", "The service did not supply an attribute in a list of attributes that was needed by the service to perform the requested behavior."},
    {0x1E, -1, PLCTAG_ERR_PARTIAL, "One or more bundled requests failed..", "One or more of the bundled requests has an error."},
    {0x1D, -1, PLCTAG_ERR_BAD_DATA, "Invalid attribute value list.", "The service is returning the list of attributes supplied with status information for those attributes that were invalid."},
    {0x20, -1, PLCTAG_ERR_BAD_PARAM, "Invalid parameter.", "A parameter associated with the request was invalid. This code is used when a parameter does meet the requirements defined in an Application Object specification."},
    {0x21, -1, PLCTAG_ERR_DUPLICATE, "Write-once value or medium already written.", "An attempt was made to write to a write-once-medium that has already been written or to modify a value that cannot be change once established."},
    {0x22, -1, PLCTAG_ERR_BAD_REPLY, "Invalid Reply Received", "An invalid reply is received (example: service code sent doesn't match service code received.)."},
    {0x25, -1, PLCTAG_ERR_BAD_PARAM, "Key failure in path", "The key segment was included as the first segment in the path does not match the destination module."},
    {0x26, -1, PLCTAG_ERR_BAD_PARAM, "The number of IOI words specified does not match IOI word count.", "Check the tag length against what was sent."},
    {0x27, -1, PLCTAG_ERR_BAD_PARAM, "Unexpected attribute in list", "An attempt was made to set an attribute that is not able to be set at this time."},
    {0x28, -1, PLCTAG_ERR_BAD_PARAM, "Invalid Member ID.", "The Member ID specified in the request does not exist in the specified Class/Instance/Attribute."},
    {0x29, -1, PLCTAG_ERR_NOT_ALLOWED, "Member not writable.", "A request to modify a non-modifiable member was received."},
    {0xFF, 0x2104, PLCTAG_ERR_OUT_OF_BOUNDS, "Address is out of range.",""},
    {0xFF, 0x2105, PLCTAG_ERR_OUT_OF_BOUNDS, "Attempt to access beyond the end of the data object.", ""},
    {0xFF, 0x2107, PLCTAG_ERR_BAD_PARAM, "The data type is invalid or not supported.", ""},
    {-1, -1, PLCTAG_ERR_REMOTE_ERR, "Unknown error code.", "Unknown error code."}
};





static int lookup_error_code(uint8_t *data)
{
    int index = 0;
    int primary_code = 0;
    int secondary_code = 0;

    /* build the error status */
    primary_code = (int)*data;

    if(primary_code != 0) {
        int num_status_words = 0;

        data++;
        num_status_words = (int)*data;

        if(num_status_words > 0) {
            data++;
            secondary_code = (int)data[0] + (int)(data[1] << 8);
        }
    }

    while(error_code_table[index].primary_code != -1) {
        if(error_code_table[index].primary_code == primary_code) {
            if(error_code_table[index].secondary_code == secondary_code || error_code_table[index].secondary_code == -1) {
                break;
            }
        }

        index++;
    }

    return index;
}




const char *decode_cip_error_short(uint8_t *data)
{
    int index = lookup_error_code(data);

    return error_code_table[index].short_desc;
}


const char *decode_cip_error_long(uint8_t *data)
{
    int index = lookup_error_code(data);

    return error_code_table[index].long_desc;
}


int decode_cip_error_code(uint8_t *data)
{
    int index = lookup_error_code(data);

    return error_code_table[index].translated_code;
}

#endif // __PROTOCOLS_AB_ERROR_CODES_C__


#ifndef __PROTOCOLS_AB_SESSION_C__
#define __PROTOCOLS_AB_SESSION_C__

#include <inttypes.h>
#include <limits.h>
#include <stdlib.h>
#include <time.h>

#define MAX_REQUESTS (200)

#define EIP_CIP_PREFIX_SIZE (44) /* bytes of encap header and CFP connected header */

/* WARNING: this must fit within 9 bits! */
#define MAX_CIP_MSG_SIZE        (0x01FF & 508)

/* Warning, this must fit within 16 bits */
#define MAX_CIP_MSG_SIZE_EX     (0xFFFF & 4002)

/* maximum for PCCC embedded within CIP. */
#define MAX_CIP_PLC5_MSG_SIZE (244)
// #define MAX_CIP_SLC_MSG_SIZE (222)
#define MAX_CIP_SLC_MSG_SIZE (244)
#define MAX_CIP_MLGX_MSG_SIZE (244)
#define MAX_CIP_OMRON_MSG_SIZE (1996)

/*
 * Number of milliseconds to wait to try to set up the session again
 * after a failure.
 */
#define RETRY_WAIT_MS (5000)

#define SESSION_DISCONNECT_TIMEOUT (5000)

#define SOCKET_WAIT_TIMEOUT_MS (20)
#define SESSION_IDLE_WAIT_TIME (100)



static ab_session_p session_create_unsafe(const char *host, const char *path, plc_type_t plc_type, int *use_connected_msg, int connection_group_id);
static int session_init(ab_session_p session);
//static int get_plc_type(attr attribs);
static int add_session_unsafe(ab_session_p n);
static int remove_session_unsafe(ab_session_p n);
static ab_session_p find_session_by_host_unsafe(const char *gateway, const char *path, int connection_group_id);
static int session_match_valid(const char *host, const char *path, ab_session_p session);
static int session_add_request_unsafe(ab_session_p sess, ab_request_p req);
static int session_open_socket(ab_session_p session);
static void session_destroy(void *session);
static int session_register(ab_session_p session);
static int session_close_socket(ab_session_p session);
static int session_unregister(ab_session_p session);
static THREAD_FUNC(session_handler);
static int purge_aborted_requests_unsafe(ab_session_p session);
static int process_requests(ab_session_p session);
//static int check_packing(ab_session_p session, ab_request_p request);
static int get_payload_size(ab_request_p request);
static int pack_requests(ab_session_p session, ab_request_p *requests, int num_requests);
static int prepare_request(ab_session_p session);
static int send_eip_request(ab_session_p session, int timeout);
static int recv_eip_response(ab_session_p session, int timeout);
static int unpack_response(ab_session_p session, ab_request_p request, int sub_packet);
// static int perform_forward_open(ab_session_p session);
static int perform_forward_close(ab_session_p session);
// static int try_forward_open_ex(ab_session_p session, int *max_payload_size_guess);
// static int try_forward_open(ab_session_p session);
// static int send_forward_open_req(ab_session_p session);
// static int send_forward_open_req_ex(ab_session_p session);
// static int recv_forward_open_resp(ab_session_p session, int *max_payload_size_guess);
static int send_forward_close_req(ab_session_p session);
static int recv_forward_close_resp(ab_session_p session);
static int send_forward_open_request(ab_session_p session);
static int send_old_forward_open_request(ab_session_p session);
static int send_extended_forward_open_request(ab_session_p session);
static int receive_forward_open_response(ab_session_p session);
static void request_destroy(void *req_arg);
static int session_request_increase_buffer(ab_request_p request, int new_capacity);


static volatile mutex_p session_mutex = NULL;
static volatile vector_p sessions = NULL;




int session_startup()
{
    int rc = PLCTAG_STATUS_OK;

    if((rc = mutex_create((mutex_p *)&session_mutex)) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to create session mutex %s!", plc_tag_decode_error(rc));
        return rc;
    }

    if((sessions = vector_create(25, 5)) == NULL) {
        pdebug(DEBUG_ERROR, "Unable to create session vector!");
        return PLCTAG_ERR_NO_MEM;
    }

    return rc;
}


void session_teardown()
{
    pdebug(DEBUG_INFO, "Starting.");

    if(sessions && session_mutex) {
        pdebug(DEBUG_DETAIL, "Waiting for sessions to terminate.");

        while(1) {
            int remaining_sessions = 0;

            critical_block(session_mutex) {
                remaining_sessions = vector_length(sessions);
            }

            /* wait for things to terminate. */
            if(remaining_sessions > 0) {
                sleep_ms(10); // MAGIC
            } else {
                break;
            }
        }

        pdebug(DEBUG_DETAIL, "Sessions all terminated.");

        vector_destroy(sessions);

        sessions = NULL;
    }

    pdebug(DEBUG_DETAIL, "Destroying session mutex.");

    if(session_mutex) {
        mutex_destroy((mutex_p *)&session_mutex);
        session_mutex = NULL;
    }

    pdebug(DEBUG_INFO, "Done.");
}







/*
 * session_get_new_seq_id_unsafe
 *
 * A wrapper to get a new session sequence ID.  Not thread safe.
 *
 * Note that this is dangerous to use in threaded applications
 * because 32-bit processors will not implement a 64-bit
 * integer as an atomic entity.
 */

uint64_t session_get_new_seq_id_unsafe(ab_session_p sess)
{
    return sess->session_seq_id++;
}

/*
 * session_get_new_seq_id
 *
 * A thread-safe function to get a new session sequence ID.
 */

uint64_t session_get_new_seq_id(ab_session_p sess)
{
    uint16_t res = 0;

    //pdebug(DEBUG_DETAIL, "entering critical block %p",session_mutex);
    critical_block(sess->mutex) {
        res = (uint16_t)session_get_new_seq_id_unsafe(sess);
    }
    //pdebug(DEBUG_DETAIL, "leaving critical block %p", session_mutex);

    return res;
}



int session_get_max_payload(ab_session_p session)
{
    int result = 0;

    if(!session) {
        pdebug(DEBUG_WARN, "Called with null session pointer!");
        return 0;
    }

    critical_block(session->mutex) {
        result = session->max_payload_size;
    }

    return result;
}

int session_find_or_create(ab_session_p *tag_session, attr attribs)
{
    /*int debug = attr_get_int(attribs,"debug",0);*/
    const char *session_gw = attr_get_str(attribs, "gateway", "");
    const char *session_path = attr_get_str(attribs, "path", "");
    int use_connected_msg = attr_get_int(attribs, "use_connected_msg", 0);
    //int session_gw_port = attr_get_int(attribs, "gateway_port", AB_EIP_DEFAULT_PORT);
    plc_type_t plc_type = get_plc_type(attribs);
    ab_session_p session = AB_SESSION_NULL;
    int new_session = 0;
    int shared_session = attr_get_int(attribs, "share_session", 1); /* share the session by default. */
    int rc = PLCTAG_STATUS_OK;
    int auto_disconnect_enabled = 0;
    int auto_disconnect_timeout_ms = INT_MAX;
    int connection_group_id = attr_get_int(attribs, "connection_group_id", 0);

    pdebug(DEBUG_DETAIL, "Starting");

    auto_disconnect_timeout_ms = attr_get_int(attribs, "auto_disconnect_ms", INT_MAX);
    if(auto_disconnect_timeout_ms != INT_MAX) {
        pdebug(DEBUG_DETAIL, "Setting auto-disconnect after %dms.", auto_disconnect_timeout_ms);
        auto_disconnect_enabled = 1;
    }

    // if(plc_type == AB_PLC_PLC5 && str_length(session_path) > 0) {
    //     /* this means it is DH+ */
    //     use_connected_msg = 1;
    //     attr_set_int(attribs, "use_connected_msg", 1);
    // }

    critical_block(session_mutex) {
        /* if we are to share sessions, then look for an existing one. */
        if (shared_session) {
            session = find_session_by_host_unsafe(session_gw, session_path, connection_group_id);
        } else {
            /* no sharing, create a new one */
            session = AB_SESSION_NULL;
        }

        if (session == AB_SESSION_NULL) {
            pdebug(DEBUG_DETAIL, "Creating new session.");
            session = session_create_unsafe(session_gw, session_path, plc_type, &use_connected_msg, connection_group_id);

            if (session == AB_SESSION_NULL) {
                pdebug(DEBUG_WARN, "unable to create or find a session!");
                rc = PLCTAG_ERR_BAD_GATEWAY;
            } else {
                session->auto_disconnect_enabled = auto_disconnect_enabled;
                session->auto_disconnect_timeout_ms = auto_disconnect_timeout_ms;

                new_session = 1;
            }
        } else {
            /* turn on auto disconnect if we need to. */
            if(!session->auto_disconnect_enabled && auto_disconnect_enabled) {
                session->auto_disconnect_enabled = auto_disconnect_enabled;
            }

            /* disconnect period always goes down. */
            if(session->auto_disconnect_enabled && session->auto_disconnect_timeout_ms > auto_disconnect_timeout_ms) {
                session->auto_disconnect_timeout_ms = auto_disconnect_timeout_ms;
            }

            pdebug(DEBUG_DETAIL, "Reusing existing session.");
        }
    }

    /*
     * do this OUTSIDE the mutex in order to let other threads not block if
     * the session creation process blocks.
     */

    if(new_session) {
        rc = session_init(session);
        if(rc != PLCTAG_STATUS_OK) {
            rc_dec(session);
            session = AB_SESSION_NULL;
        } else {
            /* save the status */
            //session->status = rc;
        }
    }

    /* store it into the tag */
    *tag_session = session;

    pdebug(DEBUG_DETAIL, "Done");

    return rc;
}




int add_session_unsafe(ab_session_p session)
{
    pdebug(DEBUG_DETAIL, "Starting");

    if (!session) {
        return PLCTAG_ERR_NULL_PTR;
    }

    vector_put(sessions, vector_length(sessions), session);

    session->on_list = 1;

    pdebug(DEBUG_DETAIL, "Done");

    return PLCTAG_STATUS_OK;
}



int add_session(ab_session_p s)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_DETAIL, "Starting.");

    critical_block(session_mutex) {
        rc = add_session_unsafe(s);
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}




int remove_session_unsafe(ab_session_p session)
{
    pdebug(DEBUG_DETAIL, "Starting");

    if (!session || !sessions) {
        return 0;
    }

    for(int i=0; i < vector_length(sessions); i++) {
        ab_session_p tmp = (ab_session_p)vector_get(sessions, i);

        if(tmp == session) {
            vector_remove(sessions, i);
            break;
        }
    }

    pdebug(DEBUG_DETAIL, "Done");

    return PLCTAG_STATUS_OK;
}

int remove_session(ab_session_p s)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(s->on_list) {
        critical_block(session_mutex) {
            rc = remove_session_unsafe(s);
        }
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}


int session_match_valid(const char *host, const char *path, ab_session_p session)
{
    if(!session) {
        return 0;
    }

    /* don't use sessions that failed immediately. */
    if(session->failed) {
        return 0;
    }

    if(!str_length(host)) {
        pdebug(DEBUG_WARN, "New session host is NULL or zero length!");
        return 0;
    }

    if(!str_length(session->host)) {
        pdebug(DEBUG_WARN, "Session host is NULL or zero length!");
        return 0;
    }

    if(str_cmp_i(host, session->host)) {
        return 0;
    }

    if(str_cmp_i(path, session->path)) {
        return 0;
    }

    return 1;
}


ab_session_p find_session_by_host_unsafe(const char *host, const char *path, int connection_group_id)
{
    for(int i=0; i < vector_length(sessions); i++) {
        ab_session_p session = (ab_session_p)vector_get(sessions, i);

        /* is this session in the process of destruction? */
        session = (ab_session_p)rc_inc(session);
        if(session) {
            if(session->connection_group_id == connection_group_id && session_match_valid(host, path, session)) {
                return session;
            }

            rc_dec(session);
        }
    }

    return NULL;
}



ab_session_p session_create_unsafe(const char *host, const char *path, plc_type_t plc_type, int *use_connected_msg, int connection_group_id)
{
    static volatile uint32_t connection_id = 0;

    int rc = PLCTAG_STATUS_OK;
    ab_session_p session = AB_SESSION_NULL;

    pdebug(DEBUG_INFO, "Starting");

    if(*use_connected_msg) {
        pdebug(DEBUG_DETAIL, "Session should use connected messaging.");
    } else {
        pdebug(DEBUG_DETAIL, "Session should not use connected messaging.");
    }

    session = (ab_session_p)rc_alloc(sizeof(struct ab_session_t), session_destroy);
    if (!session) {
        pdebug(DEBUG_WARN, "Error allocating new session.");
        return AB_SESSION_NULL;
    }

    session->host = str_dup(host);
    if(!session->host) {
        pdebug(DEBUG_WARN, "Unable to duplicate host string!");
        rc_dec(session);
        return NULL;
    }

    rc = cip_encode_path(path, use_connected_msg, plc_type, &session->conn_path, &session->conn_path_size, &session->is_dhp, &session->dhp_dest);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_INFO, "Unable to convert path links strings to binary path!");
        rc_dec(session);
        return NULL;
    }

    if(path && str_length(path)) {
        session->path = str_dup(path);
        if(path && str_length(path) && !session->path) {
            pdebug(DEBUG_WARN, "Unable to duplicate path string!");
            rc_dec(session);
            return NULL;
        }
    }

    session->requests = vector_create(SESSION_MIN_REQUESTS, SESSION_INC_REQUESTS);
    if(!session->requests) {
        pdebug(DEBUG_WARN, "Unable to allocate vector for requests!");
        rc_dec(session);
        return NULL;
    }

    /* check for ID set up. This does not need to be thread safe since we just need a random value. */
    if(connection_id == 0) {
        connection_id = (uint32_t)rand();
    }

    session->plc_type = plc_type;
    session->data_capacity = MAX_PACKET_SIZE_EX;
    session->use_connected_msg = *use_connected_msg;
    session->failed = 0;
    session->conn_serial_number = (uint16_t)(uintptr_t)(intptr_t)rand();
    session->session_seq_id = (uint64_t)rand();

    pdebug(DEBUG_DETAIL, "Setting connection_group_id to %d.", connection_group_id);
    session->connection_group_id = connection_group_id;

    /* guess the max CIP payload size. */
    switch(plc_type) {
    case AB_PLC_SLC:
        session->max_payload_size = MAX_CIP_SLC_MSG_SIZE;
        session->only_use_old_forward_open = 1;
        break;

    case AB_PLC_MLGX:
        session->max_payload_size = MAX_CIP_MLGX_MSG_SIZE;
        session->only_use_old_forward_open = 1;
        break;

    case AB_PLC_PLC5:
    case AB_PLC_LGX_PCCC:
        session->max_payload_size = MAX_CIP_PLC5_MSG_SIZE;
        session->only_use_old_forward_open = 1;
        break;

    case AB_PLC_LGX:
        session->max_payload_size = MAX_CIP_MSG_SIZE;
        break;

    case AB_PLC_MICRO800:
        session->max_payload_size = MAX_CIP_MSG_SIZE;
        break;

    case AB_PLC_OMRON_NJNX:
        session->max_payload_size = MAX_CIP_OMRON_MSG_SIZE;
        break;

    default:
        pdebug(DEBUG_WARN, "Unknown protocol/cpu type!");
        rc_dec(session);
        return NULL;
        break;
    }

    pdebug(DEBUG_DETAIL, "Set maximum payload size to %u bytes.", (unsigned int)(session->max_payload_size));

    /*
     * Why is connection_id global?  Because it looks like the PLC might
     * be treating it globally.  I am seeing ForwardOpen errors that seem
     * to be because of duplicate connection IDs even though the session
     * was closed.
     *
     * So, this is more or less unique across all invocations of the library.
     * FIXME - this could collide.  The probability is low, but it could happen
     * as there are only 32 bits.
     */
    session->orig_connection_id = ++connection_id;

    /* add the new session to the list. */
    add_session_unsafe(session);

    pdebug(DEBUG_INFO, "Done");

    return session;
}


/*
 * session_init
 *
 * This calls several blocking methods and so must not keep the main mutex
 * locked during them.
 */
int session_init(ab_session_p session)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    /* create the session mutex. */
    if((rc = mutex_create(&(session->mutex))) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to create session mutex!");
        session->failed = 1;
        return rc;
    }

    /* create the session condition variable. */
    if((rc = cond_create(&(session->wait_cond))) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to create session condition var!");
        session->failed = 1;
        return rc;
    }

    if((rc = thread_create((thread_p *)&(session->handler_thread), session_handler, 32*1024, session)) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to create session thread!");
        session->failed = 1;
        return rc;
    }

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}


/*
 * session_open_socket()
 *
 * Connect to the host/port passed via TCP.
 */

int session_open_socket(ab_session_p session)
{
    int rc = PLCTAG_STATUS_OK;
    char **server_port = NULL;
    int port = 0;

    pdebug(DEBUG_INFO, "Starting.");

    /* Open a socket for communication with the gateway. */
    rc = socket_create(&(session->sock));

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to create socket for session!");
        return rc;
    }

    server_port = str_split(session->host, ":");
    if(!server_port) {
        pdebug(DEBUG_WARN, "Unable to split server and port string!");
        return PLCTAG_ERR_BAD_CONFIG;
    }

    if(server_port[0] == NULL) {
        pdebug(DEBUG_WARN, "Server string is malformed or empty!");
        mem_free(server_port);
        return PLCTAG_ERR_BAD_CONFIG;
    }

    if(server_port[1] != NULL) {
        rc = str_to_int(server_port[1], &port);
        if(rc != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Unable to extract port number from server string \"%s\"!", session->host);
            mem_free(server_port);
            return PLCTAG_ERR_BAD_CONFIG;
        }

        pdebug(DEBUG_DETAIL, "Using special port %d.", port);
    } else {
        port = AB_EIP_DEFAULT_PORT;

        pdebug(DEBUG_DETAIL, "Using default port %d.", port);
    }

    rc = socket_connect_tcp_start(session->sock, server_port[0], port);

    if (rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
        pdebug(DEBUG_WARN, "Unable to connect socket for session!");
        mem_free(server_port);
        return rc;
    }

    if(server_port) {
        mem_free(server_port);
    }

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}



int session_register(ab_session_p session)
{
    eip_session_reg_req *req;
    eip_encap *resp;
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    /*
     * clear the session data.
     *
     * We use the receiving buffer because we do not have a request and nothing can
     * be coming in (we hope) on the socket yet.
     */
    mem_set(session->data, 0, sizeof(eip_session_reg_req));

    req = (eip_session_reg_req *)(session->data);

    /* fill in the fields of the request */
    req->encap_command = h2le16(AB_EIP_REGISTER_SESSION);
    req->encap_length = h2le16(sizeof(eip_session_reg_req) - sizeof(eip_encap));
    req->encap_session_handle = h2le32(/*session->session_handle*/ 0);
    req->encap_status = h2le32(0);
    req->encap_sender_context = h2le64((uint64_t)0);
    req->encap_options = h2le32(0);

    req->eip_version = h2le16(AB_EIP_VERSION);
    req->option_flags = h2le16(0);

    /*
     * socket ops here are _ASYNCHRONOUS_!
     *
     * This is done this way because we do not have everything
     * set up for a request to be handled by the thread.  I think.
     */

    /* send registration to the gateway */
    session->data_size = sizeof(eip_session_reg_req);
    session->data_offset = 0;

    rc = send_eip_request(session, SESSION_DEFAULT_TIMEOUT);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Error sending session registration request %s!", plc_tag_decode_error(rc));
        return rc;
    }

    /* get the response from the gateway */
    rc = recv_eip_response(session, SESSION_DEFAULT_TIMEOUT);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Error receiving session registration response %s!", plc_tag_decode_error(rc));
        return rc;
    }

    /* encap header is at the start of the buffer */
    resp = (eip_encap *)(session->data);

    /* check the response status */
    if (le2h16(resp->encap_command) != AB_EIP_REGISTER_SESSION) {
        pdebug(DEBUG_WARN, "EIP unexpected response packet type: %d!", resp->encap_command);
        return PLCTAG_ERR_BAD_DATA;
    }

    if (le2h32(resp->encap_status) != AB_EIP_OK) {
        pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(resp->encap_status));
        return PLCTAG_ERR_REMOTE_ERR;
    }

    /*
     * after all that, save the session handle, we will
     * use it in future packets.
     */
    session->session_handle = le2h32(resp->encap_session_handle);

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_OK;
}


int session_unregister(ab_session_p session)
{
    (void)session;

    pdebug(DEBUG_INFO, "Starting.");

    /* nothing to do, perhaps. */

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_OK;
}



int session_close_socket(ab_session_p session)
{
    pdebug(DEBUG_INFO, "Starting.");

    if (session->sock) {
        socket_close(session->sock);
        socket_destroy(&(session->sock));
        session->sock = NULL;
    }

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_OK;
}



void session_destroy(void *session_arg)
{
    ab_session_p session = (ab_session_p)session_arg;

    pdebug(DEBUG_INFO, "Starting.");

    if (!session) {
        pdebug(DEBUG_WARN, "Session ptr is null!");

        return;
    }

    /* so remove the session from the list so no one else can reference it. */
    remove_session(session);

    pdebug(DEBUG_INFO, "Session sent %" PRId64 " packets.", session->packet_count);

    /* terminate the session thread first. */
    session->terminating = 1;

    /* signal the condition variable in case it is waiting */
    if(session->wait_cond) {
        cond_signal(session->wait_cond);
    }

    /* get rid of the handler thread. */
    pdebug(DEBUG_DETAIL, "Destroying session thread.");
    if (session->handler_thread) {
        /* this cannot be guarded by the mutex since the session thread also locks it. */
        thread_join(session->handler_thread);

        /* FIXME - is this critical block needed? */
        critical_block(session->mutex) {
            thread_destroy(&(session->handler_thread));
            session->handler_thread = NULL;
        }
    }


    /* this needs to be handled in the mutex to prevent double frees due to queued requests. */
    critical_block(session->mutex) {
        /* close off the connection if is one. This helps the PLC clean up. */
        if (session->targ_connection_id) {
            /*
             * we do not want the internal loop to immediately
             * return, so set the flag like we are not terminating.
             * There is still a timeout that applies.
             */
            session->terminating = 0;
            perform_forward_close(session);
            session->terminating = 1;
        }

        /* try to be nice and un-register the session */
        if (session->session_handle) {
            session_unregister(session);
        }

        if (session->sock) {
            session_close_socket(session);
        }

        /* release all the requests that are in the queue. */
        if (session->requests) {
            for (int i = 0; i < vector_length(session->requests); i++) {
                rc_dec(vector_get(session->requests, i));
            }

            vector_destroy(session->requests);
            session->requests = NULL;
        }
    }

    /* we are done with the condition variable, finally destroy it. */
    pdebug(DEBUG_DETAIL, "Destroying session condition variable.");
    if(session->wait_cond) {
        cond_destroy(&(session->wait_cond));
        session->wait_cond = NULL;
    }

    /* we are done with the mutex, finally destroy it. */
    pdebug(DEBUG_DETAIL, "Destroying session mutex.");
    if(session->mutex) {
        mutex_destroy(&(session->mutex));
        session->mutex = NULL;
    }

    pdebug(DEBUG_DETAIL, "Cleaning up allocated memory for paths and host name.");
    if(session->conn_path) {
        mem_free(session->conn_path);
        session->conn_path = NULL;
    }

    if(session->path) {
        mem_free(session->path);
        session->path = NULL;
    }

    if(session->host) {
        mem_free(session->host);
        session->host = NULL;
    }

    pdebug(DEBUG_INFO, "Done.");

    return;
}




/*
 * session_add_request_unsafe
 *
 * You must hold the mutex before calling this!
 */
int session_add_request_unsafe(ab_session_p session, ab_request_p req)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(!session) {
        pdebug(DEBUG_WARN, "Session is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    req = (ab_request_p)rc_inc(req);

    if(!req) {
        pdebug(DEBUG_WARN, "Request is either null or in the process of being deleted.");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* make sure the request points to the session */

    /* insert into the requests vector */
    vector_put(session->requests, vector_length(session->requests), req);

    pdebug(DEBUG_DETAIL, "Total requests in the queue: %d", vector_length(session->requests));

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}

/*
 * session_add_request
 *
 * This is a thread-safe version of the above routine.
 */
int session_add_request(ab_session_p sess, ab_request_p req)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting. sess=%p, req=%p", sess, req);

    critical_block(sess->mutex) {
        rc = session_add_request_unsafe(sess, req);
    }

    cond_signal(sess->wait_cond);

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}


/*
 * session_remove_request_unsafe
 *
 * You must hold the mutex before calling this!
 */
int session_remove_request_unsafe(ab_session_p session, ab_request_p req)
{
    int rc = PLCTAG_STATUS_OK;
//    ab_request_p cur, prev;

    pdebug(DEBUG_INFO, "Starting.");

    if(session == NULL || req == NULL) {
        return rc;
    }

    for(int i=0; i < vector_length(session->requests); i++) {
        if(vector_get(session->requests, i) == req) {
            vector_remove(session->requests, i);
            break;
        }
    }

    /* release the request refcount */
    rc_dec(req);

    cond_signal(session->wait_cond);

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}



/*****************************************************************
 **************** Session handling functions *********************
 ****************************************************************/


typedef enum { SESSION_OPEN_SOCKET_START, SESSION_OPEN_SOCKET_WAIT, SESSION_REGISTER,
               SESSION_SEND_FORWARD_OPEN, SESSION_RECEIVE_FORWARD_OPEN, SESSION_IDLE,
               SESSION_DISCONNECT, SESSION_UNREGISTER, SESSION_CLOSE_SOCKET,
               SESSION_START_RETRY, SESSION_WAIT_RETRY, SESSION_WAIT_RECONNECT
             } session_state_t;


THREAD_FUNC(session_handler)
{
    ab_session_p session = (ab_session_p)arg;
    int rc = PLCTAG_STATUS_OK;
    session_state_t state = SESSION_OPEN_SOCKET_START;
    int64_t timeout_time = 0;
    int64_t wait_until_time = 0;
    int64_t auto_disconnect_time = time_ms() + SESSION_DISCONNECT_TIMEOUT;
    int auto_disconnect = 0;


    pdebug(DEBUG_INFO, "Starting thread for session %p", session);

    while(!session->terminating) {
        /* how long should we wait if nothing wakes us? */
        wait_until_time = time_ms() + SESSION_IDLE_WAIT_TIME;

        /*
         * Do this on every cycle.   This keeps the queue clean(ish).
         *
         * Make sure we get rid of all the aborted requests queued.
         * This keeps the overall memory usage lower.
         */

        pdebug(DEBUG_SPEW,"Critical block.");
        critical_block(session->mutex) {
            purge_aborted_requests_unsafe(session);
        }

        switch(state) {
        case SESSION_OPEN_SOCKET_START:
            pdebug(DEBUG_DETAIL, "in SESSION_OPEN_SOCKET_START state.");

            /* we must connect to the gateway*/
            rc = session_open_socket(session);
            if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
                pdebug(DEBUG_WARN, "session connect failed %s!", plc_tag_decode_error(rc));
                state = SESSION_CLOSE_SOCKET;
            } else {
                if(rc == PLCTAG_STATUS_OK) {
                    /* bump auto disconnect time into the future so that we do not accidentally disconnect immediately. */
                    auto_disconnect_time = time_ms() + SESSION_DISCONNECT_TIMEOUT;

                    pdebug(DEBUG_DETAIL, "Connect complete immediately, going to state SESSION_REGISTER.");

                    state = SESSION_REGISTER;
                } else {
                    pdebug(DEBUG_DETAIL, "Connect started, going to state SESSION_OPEN_SOCKET_WAIT.");

                    state = SESSION_OPEN_SOCKET_WAIT;
                }
            }

            /* in all cases, don't wait. */
            cond_signal(session->wait_cond);

            break;

        case SESSION_OPEN_SOCKET_WAIT:
            pdebug(DEBUG_DETAIL, "in SESSION_OPEN_SOCKET_WAIT state.");

            /* we must connect to the gateway */
            rc = socket_connect_tcp_check(session->sock, 20); /* MAGIC */
            if(rc == PLCTAG_STATUS_OK) {
                /* connected! */
                pdebug(DEBUG_INFO, "Socket connection succeeded.");

                /* calculate the disconnect time. */
                auto_disconnect_time = time_ms() + SESSION_DISCONNECT_TIMEOUT;

                state = SESSION_REGISTER;
            } else if(rc == PLCTAG_ERR_TIMEOUT) {
                pdebug(DEBUG_DETAIL, "Still waiting for connection to succeed.");

                /* don't wait more.  The TCP connect check will wait in select(). */
            } else {
                pdebug(DEBUG_WARN, "Session connect failed %s!", plc_tag_decode_error(rc));
                state = SESSION_CLOSE_SOCKET;
            }

            /* in all cases, don't wait. */
            cond_signal(session->wait_cond);

            break;

        case SESSION_REGISTER:
            pdebug(DEBUG_DETAIL, "in SESSION_REGISTER state.");

            if ((rc = session_register(session)) != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "session registration failed %s!", plc_tag_decode_error(rc));
                state = SESSION_CLOSE_SOCKET;
            } else {
                if(session->use_connected_msg) {
                    state = SESSION_SEND_FORWARD_OPEN;
                } else {
                    state = SESSION_IDLE;
                }
            }
            cond_signal(session->wait_cond);
            break;

        case SESSION_SEND_FORWARD_OPEN:
            pdebug(DEBUG_DETAIL, "in SESSION_SEND_FORWARD_OPEN state.");

            if((rc = send_forward_open_request(session)) != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Send Forward Open failed %s!", plc_tag_decode_error(rc));
                state = SESSION_UNREGISTER;
            } else {
                pdebug(DEBUG_DETAIL, "Send Forward Open succeeded, going to SESSION_RECEIVE_FORWARD_OPEN state.");
                state = SESSION_RECEIVE_FORWARD_OPEN;
            }
            cond_signal(session->wait_cond);
            break;

        case SESSION_RECEIVE_FORWARD_OPEN:
            pdebug(DEBUG_DETAIL, "in SESSION_RECEIVE_FORWARD_OPEN state.");

            if((rc = receive_forward_open_response(session)) != PLCTAG_STATUS_OK) {
                if(rc == PLCTAG_ERR_DUPLICATE) {
                    pdebug(DEBUG_DETAIL, "Duplicate connection error received, trying again with different connection ID.");
                    state = SESSION_SEND_FORWARD_OPEN;
                } else if(rc == PLCTAG_ERR_TOO_LARGE) {
                    pdebug(DEBUG_DETAIL, "Requested packet size too large, retrying with smaller size.");
                    state = SESSION_SEND_FORWARD_OPEN;
                } else if(rc == PLCTAG_ERR_UNSUPPORTED && !session->only_use_old_forward_open) {
                    /* if we got an unsupported error and we are trying with ForwardOpenEx, then try the old command. */
                    pdebug(DEBUG_DETAIL, "PLC does not support ForwardOpenEx, trying old ForwardOpen.");
                    session->only_use_old_forward_open = 1;
                    state = SESSION_SEND_FORWARD_OPEN;
                } else {
                    pdebug(DEBUG_WARN, "Receive Forward Open failed %s!", plc_tag_decode_error(rc));
                    state = SESSION_UNREGISTER;
                }
            } else {
                pdebug(DEBUG_DETAIL, "Send Forward Open succeeded, going to SESSION_IDLE state.");
                state = SESSION_IDLE;
            }
            cond_signal(session->wait_cond);
            break;

        case SESSION_IDLE:
            pdebug(DEBUG_DETAIL, "in SESSION_IDLE state.");

            /* if there is work to do, make sure we do not disconnect. */
            critical_block(session->mutex) {
                int num_reqs = vector_length(session->requests);
                if(num_reqs > 0) {
                    pdebug(DEBUG_DETAIL, "There are %d requests pending before cleanup and sending.", num_reqs);
                    auto_disconnect_time = time_ms() + SESSION_DISCONNECT_TIMEOUT;
                }
            }

            if((rc = process_requests(session)) != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Error while processing requests %s!", plc_tag_decode_error(rc));
                if(session->use_connected_msg) {
                    state = SESSION_DISCONNECT;
                } else {
                    state = SESSION_UNREGISTER;
                }
                cond_signal(session->wait_cond);
            }

            /* check if we should disconnect */
            if(auto_disconnect_time < time_ms()) {
                pdebug(DEBUG_DETAIL, "Disconnecting due to inactivity.");

                auto_disconnect = 1;

                if(session->use_connected_msg) {
                    state = SESSION_DISCONNECT;
                } else {
                    state = SESSION_UNREGISTER;
                }
                cond_signal(session->wait_cond);
            }

            /* if there is work to do, make sure we signal the condition var. */
            critical_block(session->mutex) {
                int num_reqs = vector_length(session->requests);
                if(num_reqs > 0) {
                    pdebug(DEBUG_DETAIL, "There are %d requests still pending after abort purge and sending.", num_reqs);
                    cond_signal(session->wait_cond);
                }
            }

            break;

        case SESSION_DISCONNECT:
            pdebug(DEBUG_DETAIL, "in SESSION_DISCONNECT state.");

            if((rc = perform_forward_close(session)) != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Forward close failed %s!", plc_tag_decode_error(rc));
            }

            state = SESSION_UNREGISTER;
            cond_signal(session->wait_cond);
            break;

        case SESSION_UNREGISTER:
            pdebug(DEBUG_DETAIL, "in SESSION_UNREGISTER state.");

            if((rc = session_unregister(session)) != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Unregistering session failed %s!", plc_tag_decode_error(rc));
            }

            state = SESSION_CLOSE_SOCKET;
            cond_signal(session->wait_cond);
            break;

        case SESSION_CLOSE_SOCKET:
            pdebug(DEBUG_DETAIL, "in SESSION_CLOSE_SOCKET state.");

            if((rc = session_close_socket(session)) != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Closing session socket failed %s!", plc_tag_decode_error(rc));
            }

            if(auto_disconnect) {
                state = SESSION_WAIT_RECONNECT;
            } else {
                state = SESSION_START_RETRY;
            }
            cond_signal(session->wait_cond);
            break;

        case SESSION_START_RETRY:
            pdebug(DEBUG_DETAIL, "in SESSION_START_RETRY state.");

            /* FIXME - make this a tag attribute. */
            timeout_time = time_ms() + RETRY_WAIT_MS;

            /* start waiting. */
            state = SESSION_WAIT_RETRY;

            cond_signal(session->wait_cond);
            break;

        case SESSION_WAIT_RETRY:
            pdebug(DEBUG_DETAIL, "in SESSION_WAIT_RETRY state.");

            if(timeout_time < time_ms()) {
                pdebug(DEBUG_DETAIL, "Transitioning to SESSION_OPEN_SOCKET_START.");
                state = SESSION_OPEN_SOCKET_START;
                cond_signal(session->wait_cond);
            }

            break;

        case SESSION_WAIT_RECONNECT:
            /* wait for at least one request to queue before reconnecting. */
            pdebug(DEBUG_DETAIL, "in SESSION_WAIT_RECONNECT state.");

            auto_disconnect = 0;

            /* if there is work to do, reconnect.. */
            pdebug(DEBUG_SPEW,"Critical block.");
            critical_block(session->mutex) {
                if(vector_length(session->requests) > 0) {
                    pdebug(DEBUG_DETAIL, "There are requests waiting, reopening connection to PLC.");

                    state = SESSION_OPEN_SOCKET_START;
                    cond_signal(session->wait_cond);
                }
            }

            break;


        default:
            pdebug(DEBUG_ERROR, "Unknown state %d!", state);

            /* FIXME - this logic is not complete.  We might be here without
             * a connected session or a registered session. */
            if(session->use_connected_msg) {
                state = SESSION_DISCONNECT;
            } else {
                state = SESSION_UNREGISTER;
            }

            cond_signal(session->wait_cond);
            break;
        }

        /*
         * give up the CPU a bit, but only if we are not
         * doing some linked states.
         */
        if(wait_until_time > 0) {
            int64_t time_left = wait_until_time - time_ms();

            if(time_left > 0) {
                cond_wait(session->wait_cond, (int)time_left);
            }
        }
    }

    /*
     * One last time before we exit.
     */
    pdebug(DEBUG_DETAIL,"Critical block.");
    critical_block(session->mutex) {
        purge_aborted_requests_unsafe(session);
    }

    THREAD_RETURN(0);
}



/*
 * This must be called with the session mutex held!
 */
int purge_aborted_requests_unsafe(ab_session_p session)
{
    int purge_count = 0;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    /* remove the aborted requests. */
    for(int i=0; i < vector_length(session->requests); i++) {
        request = (ab_request_p)vector_get(session->requests, i);

        /* filter out the aborts. */
        if(request && request->abort_request) {
            purge_count++;

            /* remove it from the queue. */
            vector_remove(session->requests, i);

            /* set the debug tag to the owning tag. */
            debug_set_tag_id(request->tag_id);

            pdebug(DEBUG_DETAIL, "Session thread releasing aborted request %p.", request);

            request->status = PLCTAG_ERR_ABORT;
            request->request_size = 0;
            request->resp_received = 1;

            /* release our hold on it. */
            request = (ab_request_p)rc_dec(request);

            /* vector size has changed, back up one. */
            i--;
        }
    }

    if(purge_count > 0) {
        pdebug(DEBUG_DETAIL, "Removed %d aborted requests.", purge_count);
    }

    pdebug(DEBUG_SPEW, "Done.");

    return purge_count;
}


int process_requests(ab_session_p session)
{
    int rc = PLCTAG_STATUS_OK;
    ab_request_p request = NULL;
    ab_request_p bundled_requests[MAX_REQUESTS] = {NULL};
    int num_bundled_requests = 0;
    int remaining_space = 0;

    debug_set_tag_id(0);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!session) {
        pdebug(DEBUG_WARN, "Null session pointer!");
        return PLCTAG_ERR_NULL_PTR;
    }

    pdebug(DEBUG_SPEW, "Checking for requests to process.");

    rc = PLCTAG_STATUS_OK;
    request = NULL;
    session->data_size = 0;
    session->data_offset = 0;

    /* grab a request off the front of the list. */
    critical_block(session->mutex) {
        /* is there anything to do? */
        if(vector_length(session->requests)) {
            /* get rid of all aborted requests. */
            purge_aborted_requests_unsafe(session);

            /* if there are still requests after purging all the aborted requests, process them. */

            /* how much space do we have to work with. */
            remaining_space = session->max_payload_size - (int)sizeof(cip_multi_req_header);

            if(vector_length(session->requests)) {
                do {
                    request = (ab_request_p)vector_get(session->requests, 0);

                    remaining_space = remaining_space - get_payload_size(request);

                    /*
                     * If we have a non-packable request, only queue it if it is the first one.
                     * If the request is packable, keep queuing as long as there is space.
                     */

                    if(num_bundled_requests == 0 || (request->allow_packing && remaining_space > 0)) {
                        //pdebug(DEBUG_DETAIL, "packed %d requests with remaining space %d", num_bundled_requests+1, remaining_space);
                        bundled_requests[num_bundled_requests] = request;
                        num_bundled_requests++;

                        /* remove it from the queue. */
                        vector_remove(session->requests, 0);
                    }
                } while(vector_length(session->requests) && remaining_space > 0 && num_bundled_requests < MAX_REQUESTS && request->allow_packing);
            } else {
                pdebug(DEBUG_DETAIL, "All requests in queue were aborted, nothing to do.");
            }
        }
    }

    /* output debug display as no particular tag. */
    debug_set_tag_id(0);

    if(num_bundled_requests > 0) {

        pdebug(DEBUG_INFO, "%d requests to process.", num_bundled_requests);

        do {
            /* copy and pack the requests into the session buffer. */
            rc = pack_requests(session, bundled_requests, num_bundled_requests);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Error while packing requests, %s!", plc_tag_decode_error(rc));
                break;
            }

            /* fill in all the necessary parts to the request. */
            if((rc = prepare_request(session)) != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Unable to prepare request, %s!", plc_tag_decode_error(rc));
                break;
            }

            /* send the request */
            if((rc = send_eip_request(session, SESSION_DEFAULT_TIMEOUT)) != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Error sending packet %s!", plc_tag_decode_error(rc));
                break;
            }

            /* wait for the response */
            if((rc = recv_eip_response(session, SESSION_DEFAULT_TIMEOUT)) != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Error receiving packet response %s!", plc_tag_decode_error(rc));
                break;
            }

            /*
             * check the CIP status, but only if this is a bundled
             * response.   If it is a singleton, then we pass the
             * status back to the tag.
             */
            if(num_bundled_requests > 1) {
                if(le2h16(((eip_encap *)(session->data))->encap_command) == AB_EIP_UNCONNECTED_SEND) {
                    eip_cip_uc_resp *resp = (eip_cip_uc_resp *)(session->data);
                    pdebug(DEBUG_INFO, "Received unconnected packet with session sequence ID %llx", resp->encap_sender_context);

                    /* punt if we got an overall error or it is not a partial/bundled error. */
                    if(resp->status != AB_EIP_OK && resp->status != AB_CIP_ERR_PARTIAL_ERROR) {
                        rc = decode_cip_error_code(&(resp->status));
                        pdebug(DEBUG_WARN, "Command failed! (%d/%d) %s", resp->status, rc, plc_tag_decode_error(rc));
                        break;
                    }
                } else if(le2h16(((eip_encap *)(session->data))->encap_command) == AB_EIP_CONNECTED_SEND) {
                    eip_cip_co_resp *resp = (eip_cip_co_resp *)(session->data);
                    pdebug(DEBUG_INFO, "Received connected packet with connection ID %x and sequence ID %u(%x)", le2h32(resp->cpf_orig_conn_id), le2h16(resp->cpf_conn_seq_num), le2h16(resp->cpf_conn_seq_num));

                    /* punt if we got an overall error or it is not a partial/bundled error. */
                    if(resp->status != AB_EIP_OK && resp->status != AB_CIP_ERR_PARTIAL_ERROR) {
                        rc = decode_cip_error_code(&(resp->status));
                        pdebug(DEBUG_WARN, "Command failed! (%d/%d) %s", resp->status, rc, plc_tag_decode_error(rc));
                        break;
                    }
                }
            }

            /* copy the results back out. Every request gets a copy. */
            for(int i=0; i < num_bundled_requests; i++) {
                debug_set_tag_id(bundled_requests[i]->tag_id);

                rc = unpack_response(session, bundled_requests[i], i);
                if(rc != PLCTAG_STATUS_OK) {
                    pdebug(DEBUG_WARN, "Unable to unpack response!");
                    break;
                }

                /* release our reference */
                bundled_requests[i] = (ab_request_p)rc_dec(bundled_requests[i]);
            }

            rc = PLCTAG_STATUS_OK;
        } while(0);

        /* problem? clean up the pending requests and dump everything. */
        if(rc != PLCTAG_STATUS_OK) {
            for(int i=0; i < num_bundled_requests; i++) {
                if(bundled_requests[i]) {
                    bundled_requests[i]->status = rc;
                    bundled_requests[i]->request_size = 0;
                    bundled_requests[i]->resp_received = 1;

                    bundled_requests[i] = (ab_request_p)rc_dec(bundled_requests[i]);
                }
            }
        }

        /* tickle the main tickler thread to note that we have responses. */
        plc_tag_tickler_wake();
    }

    debug_set_tag_id(0);

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}


int unpack_response(ab_session_p session, ab_request_p request, int sub_packet)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_co_resp *packed_resp = (eip_cip_co_resp *)(session->data);
    eip_cip_co_resp *unpacked_resp = NULL;
    uint8_t *pkt_start = NULL;
    uint8_t *pkt_end = NULL;
    int new_eip_len = 0;

    pdebug(DEBUG_INFO, "Starting.");

    /* clear out the request data. */
    mem_set(request->data, 0, request->request_capacity);

    /* change what we do depending on the type. */
    if(packed_resp->reply_service != (AB_EIP_CMD_CIP_MULTI | AB_EIP_CMD_CIP_OK)) {
        /* copy the data back into the request buffer. */
        new_eip_len = (int)session->data_size;
        pdebug(DEBUG_INFO, "Got single response packet.  Copying %d bytes unchanged.", new_eip_len);

        if(new_eip_len > request->request_capacity) {
            int request_capacity = 0;

            pdebug(DEBUG_INFO, "Request buffer too small, allocating larger buffer.");

            critical_block(session->mutex) {
                request_capacity = (int)(session->max_payload_size + EIP_CIP_PREFIX_SIZE);
            }

            /* make sure it will fit. */
            if(new_eip_len > request_capacity) {
                pdebug(DEBUG_WARN, "something is very wrong, packet length is %d but allowable capacity is %d!", new_eip_len, request_capacity);
                return PLCTAG_ERR_TOO_LARGE;
            }

            rc = session_request_increase_buffer(request, request_capacity);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Unable to increase request buffer size to %d bytes!", request_capacity);
                return rc;
            }
        }

        mem_copy(request->data, session->data, new_eip_len);
    } else {
        cip_multi_resp_header *multi = (cip_multi_resp_header *)(&packed_resp->reply_service);
        uint16_t total_responses = le2h16(multi->request_count);
        int pkt_len = 0;

        /* this is a packed response. */
        pdebug(DEBUG_INFO, "Got multiple response packet, subpacket %d", sub_packet);

        pdebug(DEBUG_INFO, "Our result offset is %d bytes.", (int)le2h16(multi->request_offsets[sub_packet]));

        pkt_start = ((uint8_t *)(&multi->request_count) + le2h16(multi->request_offsets[sub_packet]));

        /* calculate the end of the data. */
        if((sub_packet + 1) < total_responses) {
            /* not the last response */
            pkt_end = (uint8_t *)(&multi->request_count) + le2h16(multi->request_offsets[sub_packet + 1]);
        } else {
            pkt_end = (session->data + le2h16(packed_resp->encap_length) + sizeof(eip_encap));
        }

        pkt_len = (int)(pkt_end - pkt_start);

        /* replace the request buffer if it is not big enough. */
        new_eip_len = pkt_len + (int)sizeof(eip_cip_co_generic_response);
        if(new_eip_len > request->request_capacity) {
            int request_capacity = 0;

            pdebug(DEBUG_INFO, "Request buffer too small, allocating larger buffer.");

            critical_block(session->mutex) {
                request_capacity = (int)(session->max_payload_size + EIP_CIP_PREFIX_SIZE);
            }

            /* make sure it will fit. */
            if(new_eip_len > request_capacity) {
                pdebug(DEBUG_WARN, "something is very wrong, packet length is %d but allowable capacity is %d!", new_eip_len, request_capacity);
                return PLCTAG_ERR_TOO_LARGE;
            }

            rc = session_request_increase_buffer(request, request_capacity);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Unable to increase request buffer size to %d bytes!", request_capacity);
                return rc;
            }
        }

        /* point to the response buffer in a structured way. */
        unpacked_resp = (eip_cip_co_resp *)(request->data);

        /* copy the header down */
        mem_copy(request->data, session->data, (int)sizeof(eip_cip_co_resp));

        /* size of the new packet */
        new_eip_len = (uint16_t)(((uint8_t *)(&unpacked_resp->reply_service) + pkt_len) /* end of the packet */
                                 - (uint8_t *)(request->data));                         /* start of the packet */

        /* now copy the packet over that. */
        mem_copy(&unpacked_resp->reply_service, pkt_start, pkt_len);

        /* stitch up the packet sizes. */
        unpacked_resp->cpf_cdi_item_length = h2le16((uint16_t)(pkt_len + (int)sizeof(uint16_le))); /* extra for the connection sequence */
        unpacked_resp->encap_length = h2le16((uint16_t)(new_eip_len - (uint16_t)sizeof(eip_encap)));
    }

    pdebug(DEBUG_INFO, "Unpacked packet:");
    pdebug_dump_bytes(DEBUG_INFO, request->data, new_eip_len);

    /* notify the reading thread that the request is ready */
    spin_block(&request->lock) {
        request->status = PLCTAG_STATUS_OK;
        request->request_size = new_eip_len;
        request->resp_received = 1;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}



int get_payload_size(ab_request_p request)
{
    int request_data_size = 0;
    eip_encap *header = (eip_encap *)(request->data);
    eip_cip_co_req *co_req = NULL;

    pdebug(DEBUG_DETAIL, "Starting.");

    if(le2h16(header->encap_command) == AB_EIP_CONNECTED_SEND) {
        co_req = (eip_cip_co_req *)(request->data);
        /* get length of new request */
        request_data_size = le2h16(co_req->cpf_cdi_item_length)
                            - 2  /* for connection sequence ID */
                            + 2  /* for multipacket offset */
                            ;
    } else {
        pdebug(DEBUG_DETAIL, "Not a supported type EIP packet type %d to get the payload size.", le2h16(header->encap_command));
        request_data_size = INT_MAX;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return request_data_size;
}




int pack_requests(ab_session_p session, ab_request_p *requests, int num_requests)
{
    eip_cip_co_req *new_req = NULL;
    eip_cip_co_req *packed_req = NULL;
    /* FIXME - is this the right way to check? */
    int header_size = 0;
    cip_multi_req_header *multi_header = NULL;
    int current_offset = 0;
    uint8_t *pkt_start = NULL;
    int pkt_len = 0;
    uint8_t *first_pkt_data = NULL;
    uint8_t *next_pkt_data = NULL;

    pdebug(DEBUG_INFO, "Starting.");

    debug_set_tag_id(requests[0]->tag_id);

    /* get the header info from the first request. Just copy the whole thing. */
    mem_copy(session->data, requests[0]->data, requests[0]->request_size);
    session->data_size = (uint32_t)requests[0]->request_size;

    /* special case the case where there is just one request. */
    if(num_requests == 1) {
        pdebug(DEBUG_INFO, "Only one request, so done.");

        debug_set_tag_id(0);

        return PLCTAG_STATUS_OK;
    }

    /* set up multi-packet header. */

    header_size = (int)(sizeof(cip_multi_req_header)
                        + (sizeof(uint16_le) * (size_t)num_requests)); /* offsets for each request. */

    pdebug(DEBUG_INFO, "header size %d", header_size);

    packed_req = (eip_cip_co_req *)(session->data);

    /* make room in the request packet in the session for the header. */
    pkt_start = (uint8_t *)(&packed_req->cpf_conn_seq_num) + sizeof(packed_req->cpf_conn_seq_num);
    pkt_len = (int)le2h16(packed_req->cpf_cdi_item_length) - (int)sizeof(packed_req->cpf_conn_seq_num);

    pdebug(DEBUG_INFO, "packet 0 is of length %d.", pkt_len);

    /* point to where we want the current packet to start. */
    first_pkt_data = pkt_start + header_size;

    /* move the data over to make room */
    mem_move(first_pkt_data, pkt_start, pkt_len);

    /* now fill in the header. Use pkt_start as it is pointing to the right location. */
    multi_header = (cip_multi_req_header *)pkt_start;
    multi_header->service_code = AB_EIP_CMD_CIP_MULTI;
    multi_header->req_path_size = 0x02; /* length of path in words */
    multi_header->req_path[0] = 0x20; /* Class */
    multi_header->req_path[1] = 0x02; /* CM */
    multi_header->req_path[2] = 0x24; /* Instance */
    multi_header->req_path[3] = 0x01; /* #1 */
    multi_header->request_count = h2le16((uint16_t)num_requests);

    /* set up the offset for the first request. */
    current_offset = (int)(sizeof(uint16_le) + (sizeof(uint16_le) * (size_t)num_requests));
    multi_header->request_offsets[0] = h2le16((uint16_t)current_offset);

    next_pkt_data = first_pkt_data + pkt_len;
    current_offset = current_offset + pkt_len;

    /* now process the rest of the requests. */
    for(int i=1; i<num_requests; i++) {
        debug_set_tag_id(requests[i]->tag_id);

        /* set up the offset */
        multi_header->request_offsets[i] = h2le16((uint16_t)current_offset);

        /* get a pointer to the request. */
        new_req = (eip_cip_co_req *)(requests[i]->data);

        /* calculate the request start and length */
        pkt_start = (uint8_t *)(&new_req->cpf_conn_seq_num) + sizeof(new_req->cpf_conn_seq_num);
        pkt_len = (int)le2h16(new_req->cpf_cdi_item_length) - (int)sizeof(new_req->cpf_conn_seq_num);

        pdebug(DEBUG_INFO, "packet %d is of length %d.", i, pkt_len);

        /* copy the request into the session buffer. */
        mem_copy(next_pkt_data, pkt_start, pkt_len);

        /* calculate the next packet info. */
        next_pkt_data += pkt_len;
        current_offset += pkt_len;
    }

    /* stitch up the CPF packet length */
    packed_req->cpf_cdi_item_length = h2le16((uint16_t)(next_pkt_data - (uint8_t *)(&packed_req->cpf_conn_seq_num)));

    /* stick up the EIP packet length */
    packed_req->encap_length = h2le16((uint16_t)((size_t)(next_pkt_data - session->data) - sizeof(eip_encap)));

    /* set the total data size */
    session->data_size = (uint32_t)(next_pkt_data - session->data);

    debug_set_tag_id(0);

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_OK;
}



int prepare_request(ab_session_p session)
{
    eip_encap *encap = NULL;
    int payload_size = 0;

    pdebug(DEBUG_INFO, "Starting.");

    encap = (eip_encap *)(session->data);
    payload_size = (int)session->data_size - (int)sizeof(eip_encap);

    if(!session) {
        pdebug(DEBUG_WARN, "Called with null session!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* fill in the fields of the request. */

    encap->encap_length = h2le16((uint16_t)payload_size);
    encap->encap_session_handle = h2le32(session->session_handle);
    encap->encap_status = h2le32(0);
    encap->encap_options = h2le32(0);

    /* set up the session sequence ID for this transaction */
    if(le2h16(encap->encap_command) == AB_EIP_UNCONNECTED_SEND) {
        /* get new ID */
        session->session_seq_id++;

        //request->session_seq_id = session->session_seq_id;
        encap->encap_sender_context = h2le64(session->session_seq_id); /* link up the request seq ID and the packet seq ID */

        pdebug(DEBUG_INFO, "Preparing unconnected packet with session sequence ID %llx", session->session_seq_id);
    } else if(le2h16(encap->encap_command) == AB_EIP_CONNECTED_SEND) {
        eip_cip_co_req *conn_req = (eip_cip_co_req *)(session->data);

        pdebug(DEBUG_DETAIL, "cpf_targ_conn_id=%x", session->targ_connection_id);

        /* set up the connection information */
        conn_req->cpf_targ_conn_id = h2le32(session->targ_connection_id);

        session->conn_seq_num++;
        conn_req->cpf_conn_seq_num = h2le16(session->conn_seq_num);

        pdebug(DEBUG_INFO, "Preparing connected packet with connection ID %x and sequence ID %u(%x)", session->orig_connection_id, session->conn_seq_num, session->conn_seq_num);
    } else {
        pdebug(DEBUG_WARN, "Unsupported packet type %x!", le2h16(encap->encap_command));
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* display the data */
    pdebug(DEBUG_INFO, "Prepared packet of size %d", session->data_size);
    pdebug_dump_bytes(DEBUG_INFO, session->data, (int)session->data_size);

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_OK;
}




int send_eip_request(ab_session_p session, int timeout)
{
    int rc = PLCTAG_STATUS_OK;
    int64_t timeout_time = 0;

    pdebug(DEBUG_INFO, "Starting.");

    if(!session) {
        pdebug(DEBUG_WARN, "Session pointer is null.");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(timeout > 0) {
        timeout_time = time_ms() + timeout;
    } else {
        timeout_time = INT64_MAX;
    }

    pdebug(DEBUG_INFO, "Sending packet of size %d", session->data_size);
    pdebug_dump_bytes(DEBUG_INFO, session->data, (int)(session->data_size));

    session->data_offset = 0;
    session->packet_count++;

    /* send the packet */
    do {
        rc = socket_write(session->sock,
                          session->data + session->data_offset,
                          (int)session->data_size - (int)session->data_offset,
                          SOCKET_WAIT_TIMEOUT_MS);

        if(rc >= 0) {
            session->data_offset += (uint32_t)rc;
        } else {
            if(rc == PLCTAG_ERR_TIMEOUT) {
                pdebug(DEBUG_DETAIL, "Socket not yet ready to write.");
                rc = 0;
            }
        }

        /* give up the CPU if we still are looping */
        // if(!session->terminating && rc >= 0 && session->data_offset < session->data_size) {
        //     sleep_ms(1);
        // }
    } while(!session->terminating && rc >= 0 && session->data_offset < session->data_size && timeout_time > time_ms());

    if(session->terminating) {
        pdebug(DEBUG_WARN, "Session is terminating.");
        return PLCTAG_ERR_ABORT;
    }

    if(rc < 0) {
        pdebug(DEBUG_WARN, "Error, %d, writing socket!", rc);
        return rc;
    }

    if(timeout_time <= time_ms()) {
        pdebug(DEBUG_WARN, "Timed out waiting to send data!");
        return PLCTAG_ERR_TIMEOUT;
    }

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_OK;
}



/*
 * recv_eip_response
 *
 * Look at the passed session and read any data we can
 * to fill in a packet.  If we already have a full packet,
 * punt.
 */
int recv_eip_response(ab_session_p session, int timeout)
{
    uint32_t data_needed = 0;
    int rc = PLCTAG_STATUS_OK;
    int64_t timeout_time = 0;

    pdebug(DEBUG_INFO, "Starting.");

    if(!session) {
        pdebug(DEBUG_WARN, "Called with null session!");
        return PLCTAG_ERR_NULL_PTR;
    }


    if(timeout > 0) {
        timeout_time = time_ms() + timeout;
    } else {
        timeout_time = INT64_MAX;
    }

    session->data_offset = 0;
    session->data_size = 0;
    data_needed = sizeof(eip_encap);

    do {
        rc = socket_read(session->sock,
                         session->data + session->data_offset,
                         (int)(data_needed - session->data_offset),
                         SOCKET_WAIT_TIMEOUT_MS);

        if(rc >= 0) {
            session->data_offset += (uint32_t)rc;

            /*pdebug_dump_bytes(session->debug, session->data, session->data_offset);*/

            /* recalculate the amount of data needed if we have just completed the read of an encap header */
            if(session->data_offset >= sizeof(eip_encap)) {
                data_needed = (uint32_t)(sizeof(eip_encap) + le2h16(((eip_encap *)(session->data))->encap_length));

                if(data_needed > session->data_capacity) {
                    pdebug(DEBUG_WARN, "Packet response (%d) is larger than possible buffer size (%d)!", data_needed, session->data_capacity);
                    return PLCTAG_ERR_TOO_LARGE;
                }
            }
        } else {
            if(rc == PLCTAG_ERR_TIMEOUT) {
                pdebug(DEBUG_DETAIL, "Socket not yet ready to read.");
                rc = 0;
            } else {
                /* error! */
                pdebug(DEBUG_WARN, "Error reading socket! rc=%d", rc);
                return rc;
            }
        }

        // /* did we get all the data? */
        // if(!session->terminating && session->data_offset < data_needed) {
        //     /* do not hog the CPU */
        //     sleep_ms(1);
        // }
    } while(!session->terminating && session->data_offset < data_needed && timeout_time > time_ms());

    if(session->terminating) {
        pdebug(DEBUG_INFO, "Session is terminating, returning...");
        return PLCTAG_ERR_ABORT;
    }

    if(timeout_time <= time_ms()) {
        pdebug(DEBUG_WARN, "Timed out waiting for data to read!");
        return PLCTAG_ERR_TIMEOUT;
    }

    session->resp_seq_id = le2h64(((eip_encap *)(session->data))->encap_sender_context);
    session->data_size = data_needed;

    rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "request received all needed data (%d bytes of %d).", session->data_offset, data_needed);

    pdebug_dump_bytes(DEBUG_INFO, session->data, (int)(session->data_offset));

    /* check status. */
    if(le2h32(((eip_encap *)(session->data))->encap_status) != AB_EIP_OK) {
        rc = PLCTAG_ERR_BAD_STATUS;
    }

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}



int perform_forward_close(ab_session_p session)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    do {
        rc = send_forward_close_req(session);
        if(rc != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Sending forward close failed, %s!", plc_tag_decode_error(rc));
            break;
        }

        rc = recv_forward_close_resp(session);
        if(rc != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Forward close response not received, %s!", plc_tag_decode_error(rc));
            break;
        }
    } while(0);

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}



int send_forward_open_request(ab_session_p session)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting");

    // /* if this is the first time we are called set up a default guess size. */
    // if(!session->max_payload_guess) {
    //     if(session->plc_type == AB_PLC_LGX && session->use_connected_msg && !session->only_use_old_forward_open) {
    //         session->max_payload_guess = MAX_CIP_MSG_SIZE_EX;
    //     } else {
    //         session->max_payload_guess = session->max_payload_size;
    //     }

    //     pdebug(DEBUG_DETAIL, "Setting initial payload size guess to %u.", (unsigned int)session->max_payload_guess);
    // }

    // /*
    //  * double check the message size.   If we just transitioned to using the old ForwardOpen command
    //  * then the maximum payload guess might be too large.
    //  */
    // if((session->plc_type == AB_PLC_LGX || session->plc_type == AB_PLC_OMRON_NJNX) && session->only_use_old_forward_open && session->max_payload_guess > MAX_CIP_MSG_SIZE) {
    //     session->max_payload_guess = MAX_CIP_MSG_SIZE;
    // }

    if(session->only_use_old_forward_open) {
        uint16_t max_payload = 0;

        switch(session->plc_type) {
            case AB_PLC_PLC5:
            case AB_PLC_LGX_PCCC:
                max_payload = (uint16_t)MAX_CIP_PLC5_MSG_SIZE;
                break;

            case AB_PLC_SLC:
            case AB_PLC_MLGX:
                max_payload = (uint16_t)MAX_CIP_SLC_MSG_SIZE;
                break;

            case AB_PLC_MICRO800:
            case AB_PLC_LGX:
            case AB_PLC_OMRON_NJNX:
                max_payload = (uint16_t)MAX_CIP_MSG_SIZE;
                break;

            default:
                pdebug(DEBUG_WARN, "Unsupported PLC type %d!", session->plc_type);
                return PLCTAG_ERR_UNSUPPORTED;
                break;
        }

        /* set the max payload guess if it is larger than the maximum possible or if it is zero. */
        session->max_payload_guess = ((session->max_payload_guess == 0) || (session->max_payload_guess > max_payload) ? max_payload : session->max_payload_guess);

        pdebug(DEBUG_DETAIL, "Set maximum payload size guess to %d bytes.", session->max_payload_guess);

        rc = send_old_forward_open_request(session);
    } else {
        uint16_t max_payload = 0;

        switch(session->plc_type) {
            case AB_PLC_PLC5:
            case AB_PLC_LGX_PCCC:
            case AB_PLC_SLC:
            case AB_PLC_MLGX:
                pdebug(DEBUG_WARN, "PCCC PLCs do not support extended Forward Open!");
                return PLCTAG_ERR_UNSUPPORTED;
                break;

            case AB_PLC_LGX:
            case AB_PLC_MICRO800:
                max_payload = (uint16_t)MAX_CIP_MSG_SIZE_EX;
                break;

            case AB_PLC_OMRON_NJNX:
                max_payload = (uint16_t)MAX_CIP_OMRON_MSG_SIZE;
                break;

            default:
                pdebug(DEBUG_WARN, "Unsupported PLC type %d!", session->plc_type);
                return PLCTAG_ERR_UNSUPPORTED;
                break;
        }

        /* set the max payload guess if it is larger than the maximum possible or if it is zero. */
        session->max_payload_guess = ((session->max_payload_guess == 0) || (session->max_payload_guess > max_payload) ? max_payload : session->max_payload_guess);

        pdebug(DEBUG_DETAIL, "Set maximum payload size guess to %d bytes.", session->max_payload_guess);

        rc = send_extended_forward_open_request(session);
    }

    pdebug(DEBUG_INFO, "Done");

    return rc;
}


int send_old_forward_open_request(ab_session_p session)
{
    eip_forward_open_request_t *fo = NULL;
    uint8_t *data;
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting");

    mem_set(session->data, 0, (int)(sizeof(*fo) + session->conn_path_size));

    fo = (eip_forward_open_request_t *)(session->data);

    /* point to the end of the struct */
    data = (session->data) + sizeof(eip_forward_open_request_t);

    /* set up the path information. */
    mem_copy(data, session->conn_path, session->conn_path_size);
    data += session->conn_path_size;

    /* fill in the static parts */

    /* encap header parts */
    fo->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND); /* 0x006F EIP Send RR Data command */
    fo->encap_length = h2le16((uint16_t)(data - (uint8_t *)(&fo->interface_handle))); /* total length of packet except for encap header */
    fo->encap_session_handle = h2le32(session->session_handle);
    fo->encap_sender_context = h2le64(++session->session_seq_id);
    fo->router_timeout = h2le16(1);                       /* one second is enough ? */

    /* CPF parts */
    fo->cpf_item_count = h2le16(2);                  /* ALWAYS 2 */
    fo->cpf_nai_item_type = h2le16(AB_EIP_ITEM_NAI); /* null address item type */
    fo->cpf_nai_item_length = h2le16(0);             /* no data, zero length */
    fo->cpf_udi_item_type = h2le16(AB_EIP_ITEM_UDI); /* unconnected data item, 0x00B2 */
    fo->cpf_udi_item_length = h2le16((uint16_t)(data - (uint8_t *)(&fo->cm_service_code))); /* length of remaining data in UC data item */

    /* Connection Manager parts */
    fo->cm_service_code = AB_EIP_CMD_FORWARD_OPEN; /* 0x54 Forward Open Request or 0x5B for Forward Open Extended */
    fo->cm_req_path_size = 2;                      /* size of path in 16-bit words */
    fo->cm_req_path[0] = 0x20;                     /* class */
    fo->cm_req_path[1] = 0x06;                     /* CM class */
    fo->cm_req_path[2] = 0x24;                     /* instance */
    fo->cm_req_path[3] = 0x01;                     /* instance 1 */

    /* Forward Open Params */
    fo->secs_per_tick = AB_EIP_SECS_PER_TICK;         /* seconds per tick, no used? */
    fo->timeout_ticks = AB_EIP_TIMEOUT_TICKS;         /* timeout = srd_secs_per_tick * src_timeout_ticks, not used? */
    fo->orig_to_targ_conn_id = h2le32(0);             /* is this right?  Our connection id on the other machines? */
    fo->targ_to_orig_conn_id = h2le32(session->orig_connection_id); /* Our connection id in the other direction. */
    /* this might need to be globally unique */
    fo->conn_serial_number = h2le16(++(session->conn_serial_number)); /* our connection SEQUENCE number. */
    fo->orig_vendor_id = h2le16(AB_EIP_VENDOR_ID);               /* our unique :-) vendor ID */
    fo->orig_serial_number = h2le32(AB_EIP_VENDOR_SN);           /* our serial number. */
    fo->conn_timeout_multiplier = AB_EIP_TIMEOUT_MULTIPLIER;     /* timeout = mult * RPI */

    fo->orig_to_targ_rpi = h2le32(AB_EIP_RPI); /* us to target RPI - Request Packet Interval in microseconds */

    /* screwy logic if this is a DH+ route! */
    if((session->plc_type == AB_PLC_PLC5 || session->plc_type == AB_PLC_SLC || session->plc_type == AB_PLC_MLGX) && session->is_dhp) {
        fo->orig_to_targ_conn_params = h2le16(AB_EIP_PLC5_PARAM);
    } else {
        fo->orig_to_targ_conn_params = h2le16(AB_EIP_CONN_PARAM | session->max_payload_guess); /* packet size and some other things, based on protocol/cpu type */
    }

    fo->targ_to_orig_rpi = h2le32(AB_EIP_RPI); /* target to us RPI - not really used for explicit messages? */

    /* screwy logic if this is a DH+ route! */
    if((session->plc_type == AB_PLC_PLC5 || session->plc_type == AB_PLC_SLC || session->plc_type == AB_PLC_MLGX) && session->is_dhp) {
        fo->targ_to_orig_conn_params = h2le16(AB_EIP_PLC5_PARAM);
    } else {
        fo->targ_to_orig_conn_params = h2le16(AB_EIP_CONN_PARAM | session->max_payload_guess); /* packet size and some other things, based on protocol/cpu type */
    }

    fo->transport_class = AB_EIP_TRANSPORT_CLASS_T3; /* 0xA3, server transport, class 3, application trigger */
    fo->path_size = session->conn_path_size/2; /* size in 16-bit words */

    /* set the size of the request */
    session->data_size = (uint32_t)(data - (session->data));

    rc = send_eip_request(session, 0);

    pdebug(DEBUG_INFO, "Done");

    return rc;
}


/* new version of Forward Open */
int send_extended_forward_open_request(ab_session_p session)
{
    eip_forward_open_request_ex_t *fo = NULL;
    uint8_t *data;
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting");

    mem_set(session->data, 0, (int)(sizeof(*fo) + session->conn_path_size));

    fo = (eip_forward_open_request_ex_t *)(session->data);

    /* point to the end of the struct */
    data = (session->data) + sizeof(*fo);

    /* set up the path information. */
    mem_copy(data, session->conn_path, session->conn_path_size);
    data += session->conn_path_size;

    /* fill in the static parts */

    /* encap header parts */
    fo->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND); /* 0x006F EIP Send RR Data command */
    fo->encap_length = h2le16((uint16_t)(data - (uint8_t *)(&fo->interface_handle))); /* total length of packet except for encap header */
    fo->encap_session_handle = h2le32(session->session_handle);
    fo->encap_sender_context = h2le64(++session->session_seq_id);
    fo->router_timeout = h2le16(1);                       /* one second is enough ? */

    /* CPF parts */
    fo->cpf_item_count = h2le16(2);                  /* ALWAYS 2 */
    fo->cpf_nai_item_type = h2le16(AB_EIP_ITEM_NAI); /* null address item type */
    fo->cpf_nai_item_length = h2le16(0);             /* no data, zero length */
    fo->cpf_udi_item_type = h2le16(AB_EIP_ITEM_UDI); /* unconnected data item, 0x00B2 */
    fo->cpf_udi_item_length = h2le16((uint16_t)(data - (uint8_t *)(&fo->cm_service_code))); /* length of remaining data in UC data item */

    /* Connection Manager parts */
    fo->cm_service_code = AB_EIP_CMD_FORWARD_OPEN_EX; /* 0x54 Forward Open Request or 0x5B for Forward Open Extended */
    fo->cm_req_path_size = 2;                      /* size of path in 16-bit words */
    fo->cm_req_path[0] = 0x20;                     /* class */
    fo->cm_req_path[1] = 0x06;                     /* CM class */
    fo->cm_req_path[2] = 0x24;                     /* instance */
    fo->cm_req_path[3] = 0x01;                     /* instance 1 */

    /* Forward Open Params */
    fo->secs_per_tick = AB_EIP_SECS_PER_TICK;         /* seconds per tick, no used? */
    fo->timeout_ticks = AB_EIP_TIMEOUT_TICKS;         /* timeout = srd_secs_per_tick * src_timeout_ticks, not used? */
    fo->orig_to_targ_conn_id = h2le32(0);             /* is this right?  Our connection id on the other machines? */
    fo->targ_to_orig_conn_id = h2le32(session->orig_connection_id); /* Our connection id in the other direction. */
    /* this might need to be globally unique */
    fo->conn_serial_number = h2le16(++(session->conn_serial_number)); /* our connection ID/serial number. */
    fo->orig_vendor_id = h2le16(AB_EIP_VENDOR_ID);               /* our unique :-) vendor ID */
    fo->orig_serial_number = h2le32(AB_EIP_VENDOR_SN);           /* our serial number. */
    fo->conn_timeout_multiplier = AB_EIP_TIMEOUT_MULTIPLIER;     /* timeout = mult * RPI */
    fo->orig_to_targ_rpi = h2le32(AB_EIP_RPI); /* us to target RPI - Request Packet Interval in microseconds */
    fo->orig_to_targ_conn_params_ex = h2le32(AB_EIP_CONN_PARAM_EX | session->max_payload_guess); /* packet size and some other things, based on protocol/cpu type */
    fo->targ_to_orig_rpi = h2le32(AB_EIP_RPI); /* target to us RPI - not really used for explicit messages? */
    fo->targ_to_orig_conn_params_ex = h2le32(AB_EIP_CONN_PARAM_EX | session->max_payload_guess); /* packet size and some other things, based on protocol/cpu type */
    fo->transport_class = AB_EIP_TRANSPORT_CLASS_T3; /* 0xA3, server transport, class 3, application trigger */
    fo->path_size = session->conn_path_size/2; /* size in 16-bit words */

    /* set the size of the request */
    session->data_size = (uint32_t)(data - (session->data));

    rc = send_eip_request(session, SESSION_DEFAULT_TIMEOUT);

    pdebug(DEBUG_INFO, "Done");

    return rc;
}




int receive_forward_open_response(ab_session_p session)
{
    eip_forward_open_response_t *fo_resp;
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting");

    rc = recv_eip_response(session, 0);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to receive Forward Open response.");
        return rc;
    }

    fo_resp = (eip_forward_open_response_t *)(session->data);

    do {
        if(le2h16(fo_resp->encap_command) != AB_EIP_UNCONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", fo_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if(le2h32(fo_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", fo_resp->encap_status);
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(fo_resp->general_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "Forward Open command failed, response code: %s (%d)", decode_cip_error_short(&fo_resp->general_status), fo_resp->general_status);
            if(fo_resp->general_status == AB_CIP_ERR_UNSUPPORTED_SERVICE) {
                /* this type of command is not supported! */
                pdebug(DEBUG_WARN, "Received CIP command unsupported error from the PLC!");
                rc = PLCTAG_ERR_UNSUPPORTED;
            } else {
                rc = PLCTAG_ERR_REMOTE_ERR;

                if(fo_resp->general_status == 0x01 && fo_resp->status_size >= 2) {
                    /* we might have an error that tells us the actual size to use. */
                    uint8_t *data = &fo_resp->status_size;
                    int extended_status = data[1] | (data[2] << 8);
                    uint16_t supported_size = (uint16_t)((uint16_t)data[3] | (uint16_t)((uint16_t)data[4] << (uint16_t)8));

                    if(extended_status == 0x109) { /* MAGIC */
                        pdebug(DEBUG_WARN, "Error from forward open request, unsupported size, but size %d is supported.", supported_size);
                        session->max_payload_guess = supported_size;
                        rc = PLCTAG_ERR_TOO_LARGE;
                    } else if(extended_status == 0x100) { /* MAGIC */
                        pdebug(DEBUG_WARN, "Error from forward open request, duplicate connection ID.  Need to try again.");
                        rc = PLCTAG_ERR_DUPLICATE;
                    } else {
                        pdebug(DEBUG_WARN, "CIP extended error %s (%s)!", decode_cip_error_short(&fo_resp->general_status), decode_cip_error_long(&fo_resp->general_status));
                    }
                } else {
                    pdebug(DEBUG_WARN, "CIP error code %s (%s)!", decode_cip_error_short(&fo_resp->general_status), decode_cip_error_long(&fo_resp->general_status));
                }
            }

            break;
        }

        /* success! */
        session->targ_connection_id = le2h32(fo_resp->orig_to_targ_conn_id);
        session->orig_connection_id = le2h32(fo_resp->targ_to_orig_conn_id);

        session->max_payload_size = session->max_payload_guess;

        pdebug(DEBUG_INFO, "ForwardOpen succeeded with our connection ID %x and the PLC connection ID %x with packet size %u.", session->orig_connection_id, session->targ_connection_id, session->max_payload_size);

        rc = PLCTAG_STATUS_OK;
    } while(0);

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}


int send_forward_close_req(ab_session_p session)
{
    eip_forward_close_req_t *fc;
    uint8_t *data;
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting");

    fc = (eip_forward_close_req_t *)(session->data);

    /* point to the end of the struct */
    data = (session->data) + sizeof(*fc);

    /* set up the path information. */
    mem_copy(data, session->conn_path, session->conn_path_size);
    data += session->conn_path_size;

    /* FIXME DEBUG */
    pdebug(DEBUG_DETAIL, "Forward Close connection path:");
    pdebug_dump_bytes(DEBUG_DETAIL, session->conn_path, session->conn_path_size);

    /* fill in the static parts */

    /* encap header parts */
    fc->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND); /* 0x006F EIP Send RR Data command */
    fc->encap_length = h2le16((uint16_t)(data - (uint8_t *)(&fc->interface_handle))); /* total length of packet except for encap header */
    fc->encap_sender_context = h2le64(++session->session_seq_id);
    fc->router_timeout = h2le16(1);                       /* one second is enough ? */

    /* CPF parts */
    fc->cpf_item_count = h2le16(2);                  /* ALWAYS 2 */
    fc->cpf_nai_item_type = h2le16(AB_EIP_ITEM_NAI); /* null address item type */
    fc->cpf_nai_item_length = h2le16(0);             /* no data, zero length */
    fc->cpf_udi_item_type = h2le16(AB_EIP_ITEM_UDI); /* unconnected data item, 0x00B2 */
    fc->cpf_udi_item_length = h2le16((uint16_t)(data - (uint8_t *)(&fc->cm_service_code))); /* length of remaining data in UC data item */

    /* Connection Manager parts */
    fc->cm_service_code = AB_EIP_CMD_FORWARD_CLOSE;/* 0x4E Forward Close Request */
    fc->cm_req_path_size = 2;                      /* size of path in 16-bit words */
    fc->cm_req_path[0] = 0x20;                     /* class */
    fc->cm_req_path[1] = 0x06;                     /* CM class */
    fc->cm_req_path[2] = 0x24;                     /* instance */
    fc->cm_req_path[3] = 0x01;                     /* instance 1 */

    /* Forward Open Params */
    fc->secs_per_tick = AB_EIP_SECS_PER_TICK;         /* seconds per tick, no used? */
    fc->timeout_ticks = AB_EIP_TIMEOUT_TICKS;         /* timeout = srd_secs_per_tick * src_timeout_ticks, not used? */
    fc->conn_serial_number = h2le16(session->conn_serial_number); /* our connection SEQUENCE number. */
    fc->orig_vendor_id = h2le16(AB_EIP_VENDOR_ID);               /* our unique :-) vendor ID */
    fc->orig_serial_number = h2le32(AB_EIP_VENDOR_SN);           /* our serial number. */
    fc->path_size = session->conn_path_size/2; /* size in 16-bit words */
    fc->reserved = (uint8_t)0; /* padding for the path. */

    /* set the size of the request */
    session->data_size = (uint32_t)(data - (session->data));

    rc = send_eip_request(session, 100);

    pdebug(DEBUG_INFO, "Done");

    return rc;
}


int recv_forward_close_resp(ab_session_p session)
{
    eip_forward_close_resp_t *fo_resp;
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting");

    rc = recv_eip_response(session, 150);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to receive Forward Close response, %s!", plc_tag_decode_error(rc));
        return rc;
    }

    fo_resp = (eip_forward_close_resp_t *)(session->data);

    do {
        if(le2h16(fo_resp->encap_command) != AB_EIP_UNCONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", fo_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if(le2h32(fo_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", fo_resp->encap_status);
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        if(fo_resp->general_status != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "Forward Close command failed, response code: %d", fo_resp->general_status);
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        pdebug(DEBUG_INFO, "Connection close succeeded.");

        rc = PLCTAG_STATUS_OK;
    } while(0);

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}



int session_create_request(ab_session_p session, int tag_id, ab_request_p *req)
{
    int rc = PLCTAG_STATUS_OK;
    ab_request_p res;
    size_t request_capacity = 0;
    uint8_t *buffer = NULL;

    critical_block(session->mutex) {
        request_capacity = (size_t)(session->max_payload_size + EIP_CIP_PREFIX_SIZE);
    }

    pdebug(DEBUG_DETAIL, "Starting.");

    buffer = (uint8_t *)mem_alloc((int)request_capacity);
    if(!buffer) {
        pdebug(DEBUG_WARN, "Unable to allocate request buffer!");
        *req = NULL;
        return PLCTAG_ERR_NO_MEM;
    }

    res = (ab_request_p)rc_alloc((int)sizeof(struct ab_request_t), request_destroy);
    if (!res) {
        mem_free(buffer);
        *req = NULL;
        rc = PLCTAG_ERR_NO_MEM;
    } else {
        res->data = buffer;
        res->tag_id = tag_id;
        res->request_capacity = (int)request_capacity;
        res->lock = LOCK_INIT;

        *req = res;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}





/*
 * request_destroy
 *
 * The request must be removed from any lists before this!
 */

void request_destroy(void *req_arg)
{
    ab_request_p req = (ab_request_p)req_arg;

    pdebug(DEBUG_DETAIL, "Starting.");

    req->abort_request = 1;

    if(req->data) {
        mem_free(req->data);
        req->data = NULL;
    }

    pdebug(DEBUG_DETAIL, "Done.");
}


int session_request_increase_buffer(ab_request_p request, int new_capacity)
{
    uint8_t *old_buffer = NULL;
    uint8_t *new_buffer = NULL;

    pdebug(DEBUG_DETAIL, "Starting.");

    new_buffer = (uint8_t *)mem_alloc(new_capacity);
    if(!new_buffer) {
        pdebug(DEBUG_WARN, "Unable to allocate larger request buffer!");
        return PLCTAG_ERR_NO_MEM;
    }

    spin_block(&request->lock) {
        old_buffer = request->data;
        request->request_capacity = new_capacity;
        request->data = new_buffer;
    }

    mem_free(old_buffer);

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}

#endif // __PROTOCOLS_AB_SESSION_C__


#ifndef __LIB_INIT_C__
#define __LIB_INIT_C__

#include <stdlib.h>

/*
 * The following maps attributes to the tag creation functions.
 */


struct {
    const char *protocol;
    const char *make;
    const char *family;
    const char *model;
    const tag_create_function tag_constructor;
} tag_type_map[] = {
    /* System tags */
    {NULL, "system", "library", NULL, system_tag_create},
    /* Allen-Bradley PLCs */
    {"ab-eip", NULL, NULL, NULL, ab_tag_create},
    {"ab_eip", NULL, NULL, NULL, ab_tag_create},
    {"modbus-tcp", NULL, NULL, NULL, mb_tag_create},
    {"modbus_tcp", NULL, NULL, NULL, mb_tag_create}
};

static lock_t library_initialization_lock = LOCK_INIT;
static volatile int library_initialized = 0;
static volatile mutex_p lib_mutex = NULL;


/*
 * find_tag_create_func()
 *
 * Find an appropriate tag creation function.  This scans through the array
 * above to find a matching tag creation type.  The first match is returned.
 * A passed set of options will match when all non-null entries in the list
 * match.  This means that matches must be ordered from most to least general.
 *
 * Note that the protocol is used if it exists otherwise, the make family and
 * model will be used.
 */

tag_create_function find_tag_create_func(attr attributes)
{
    int i = 0;
    const char *protocol = attr_get_str(attributes, "protocol", NULL);
    const char *make = attr_get_str(attributes, "make", attr_get_str(attributes, "manufacturer", NULL));
    const char *family = attr_get_str(attributes, "family", NULL);
    const char *model = attr_get_str(attributes, "model", NULL);
    int num_entries = (sizeof(tag_type_map)/sizeof(tag_type_map[0]));

    /* if protocol is set, then use it to match. */
    if(protocol && str_length(protocol) > 0) {
        for(i=0; i < num_entries; i++) {
            if(tag_type_map[i].protocol && str_cmp(tag_type_map[i].protocol, protocol) == 0) {
                pdebug(DEBUG_INFO,"Matched protocol=%s", protocol);
                return tag_type_map[i].tag_constructor;
            }
        }
    } else {
        /* match make/family/model */
        for(i=0; i < num_entries; i++) {
            if(tag_type_map[i].make && make && str_cmp_i(tag_type_map[i].make, make) == 0) {
                pdebug(DEBUG_INFO,"Matched make=%s",make);
                if(tag_type_map[i].family) {
                    if(family && str_cmp_i(tag_type_map[i].family, family) == 0) {
                        pdebug(DEBUG_INFO, "Matched make=%s family=%s", make, family);
                        if(tag_type_map[i].model) {
                            if(model && str_cmp_i(tag_type_map[i].model, model) == 0) {
                                pdebug(DEBUG_INFO, "Matched make=%s family=%s model=%s", make, family, model);
                                return tag_type_map[i].tag_constructor;
                            }
                        } else {
                            /* matches until a NULL */
                            pdebug(DEBUG_INFO, "Matched make=%s family=%s model=NULL", make, family);
                            return tag_type_map[i].tag_constructor;
                        }
                    }
                } else {
                    /* matched until a NULL, so we matched */
                    pdebug(DEBUG_INFO, "Matched make=%s family=NULL model=NULL", make);
                    return tag_type_map[i].tag_constructor;
                }
            }
        }
    }

    /* no match */
    return NULL;
}


/*
 * destroy_modules() is called when the main process exits.
 *
 * Modify this for any PLC/protocol that needs to have something
 * torn down at the end.
 */

void destroy_modules(void)
{
    ab_teardown();

    mb_teardown();

    lib_teardown();

    spin_block(&library_initialization_lock) {
        if(lib_mutex != NULL) {
            /* FIXME casting to get rid of volatile is WRONG */
            mutex_destroy((mutex_p *)&lib_mutex);
            lib_mutex = NULL;
        }
    }

    plc_tag_unregister_logger();

    library_initialized = 0;
}



/*
 * initialize_modules() is called the first time any kind of tag is
 * created.  It will be called before the tag creation routines are
 * run.
 */


int initialize_modules(void)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    /*
     * Try to keep busy waiting to a minimum.
     * If there is no mutex set up, then create one.
     * Only one thread allowed at a time through this gate.
     */
    spin_block(&library_initialization_lock) {
        if(lib_mutex == NULL) {
            pdebug(DEBUG_INFO, "Creating library mutex.");
            /* FIXME - casting to get rid of volatile is WRONG */
            rc = mutex_create((mutex_p *)&lib_mutex);
        }
    }

    /* check the status outside the lock. */
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to initialize library mutex!  Error %s!", plc_tag_decode_error(rc));
        return rc;
    } else {
        /*
        * guard library initialization with a mutex.
        *
        * This prevents busy waiting as would happen with just a spin lock.
        */
        critical_block(lib_mutex) {
            if(!library_initialized) {
                /* initialize a random seed value. */
                srand((unsigned int)time_ms());

                pdebug(DEBUG_INFO,"Initializing library modules.");
                rc = lib_init();

                pdebug(DEBUG_INFO,"Initializing AB module.");
                if(rc == PLCTAG_STATUS_OK) {
                    rc = ab_init();
                }

                pdebug(DEBUG_INFO,"Initializing Modbus module.");
                if(rc == PLCTAG_STATUS_OK) {
                    rc = mb_init();
                }

                /* hook the destructor */
                atexit(plc_tag_shutdown);

                /* do this last */
                library_initialized = 1;

                pdebug(DEBUG_INFO,"Done initializing library modules.");
            }
        }
    }

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}

#endif // __LIB_INIT_C__


#ifndef __LIB_LIB_C__
#define __LIB_LIB_C__

//#define LIBPLCTAGDLL_EXPORTS 1

#include <ctype.h>
#include <float.h>
#include <inttypes.h>
#include <limits.h>
#include <stdlib.h>

#define INITIAL_TAG_TABLE_SIZE (201)

#define TAG_ID_MASK (0xFFFFFFF)

#define MAX_TAG_MAP_ATTEMPTS (50)

/* these are only internal to the file */

static volatile int32_t next_tag_id = 10; /* MAGIC */
static volatile hashtable_p tags = NULL;
static mutex_p tag_lookup_mutex = NULL;

static atomic_int library_terminating = {0};
static thread_p tag_tickler_thread = NULL;
static cond_p tag_tickler_wait = NULL;
#define TAG_TICKLER_TIMEOUT_MS  (100)
#define TAG_TICKLER_TIMEOUT_MIN_MS (10)
static int64_t tag_tickler_wait_timeout_end = 0;

//static mutex_p global_library_mutex = NULL;



/* helper functions. */
static plc_tag_p lookup_tag(int32_t id);
static int add_tag_lookup(plc_tag_p tag);
static int tag_id_inc(int id);
static THREAD_FUNC(tag_tickler_func);
static int set_tag_byte_order(plc_tag_p tag, attr attribs);
static int check_byte_order_str(const char *byte_order, int length);
// static int get_string_count_size_unsafe(plc_tag_p tag, int offset);
static int get_string_length_unsafe(plc_tag_p tag, int offset);
// static int get_string_capacity_unsafe(plc_tag_p tag, int offset);
// static int get_string_padding_unsafe(plc_tag_p tag, int offset);
// static int get_string_total_length_unsafe(plc_tag_p tag, int offset);
// static int get_string_byte_swapped_index_unsafe(plc_tag_p tag, int offset, int char_index);

#ifdef LIPLCTAGDLL_EXPORTS
    #if defined(_WIN32) || (defined(_WIN64)
#include <process.h>
BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpvReserved)
{
    switch(fdwReason) {
        case DLL_PROCESS_ATTACH:
            //fprintf(stderr, "DllMain called with DLL_PROCESS_ATTACH\n");
            break;

        case DLL_PROCESS_DETACH:
            //fprintf(stderr, "DllMain called with DLL_PROCESS_DETACH\n");
            plc_tag_shutdown();
            break;

        case DLL_THREAD_ATTACH:
            //fprintf(stderr, "DllMain called with DLL_THREAD_ATTACH\n");
            break;

        case DLL_THREAD_DETACH:
            //fprintf(stderr, "DllMain called with DLL_THREAD_DETACH\n");
            break;

        default:
            //fprintf(stderr, "DllMain called with unexpected code %d!\n", fdwReason);
            break;
    }

    return TRUE;
}
    #endif
#endif

/*
 * Initialize the library.  This is called in a threadsafe manner and
 * only called once.
 */

int lib_init(void)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    atomic_set(&library_terminating, 0);

    pdebug(DEBUG_INFO,"Setting up global library data.");

    pdebug(DEBUG_INFO,"Creating tag hashtable.");
    if((tags = hashtable_create(INITIAL_TAG_TABLE_SIZE)) == NULL) { /* MAGIC */
        pdebug(DEBUG_ERROR, "Unable to create tag hashtable!");
        return PLCTAG_ERR_NO_MEM;
    }

    pdebug(DEBUG_INFO,"Creating tag hashtable mutex.");
    rc = mutex_create((mutex_p *)&tag_lookup_mutex);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to create tag hashtable mutex!");
    }

    pdebug(DEBUG_INFO,"Creating tag condition variable.");
    rc = cond_create((cond_p *)&tag_tickler_wait);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to create tag condition var!");
    }

    pdebug(DEBUG_INFO,"Creating tag tickler thread.");
    rc = thread_create(&tag_tickler_thread, tag_tickler_func, 32*1024, NULL);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to create tag tickler thread!");
    }

    pdebug(DEBUG_INFO,"Done.");

    return rc;
}




void lib_teardown(void)
{
    pdebug(DEBUG_INFO,"Tearing down library.");

    atomic_set(&library_terminating, 1);

    if(tag_tickler_wait) {
        pdebug(DEBUG_INFO, "Signaling tag tickler condition var.");
        cond_signal(tag_tickler_wait);
    }

    if(tag_tickler_thread) {
        pdebug(DEBUG_INFO,"Tearing down tag tickler thread.");
        thread_join(tag_tickler_thread);
        thread_destroy(&tag_tickler_thread);
        tag_tickler_thread = NULL;
    }

    if(tag_tickler_wait) {
        pdebug(DEBUG_INFO, "Tearing down tag tickler condition var.");
        cond_destroy(&tag_tickler_wait);
        tag_tickler_wait = NULL;
    }

    if(tag_lookup_mutex) {
        pdebug(DEBUG_INFO,"Tearing down tag lookup mutex.");
        mutex_destroy(&tag_lookup_mutex);
        tag_lookup_mutex = NULL;
    }

    if(tags) {
        pdebug(DEBUG_INFO, "Destroying tag hashtable.");
        hashtable_destroy(tags);
        tags = NULL;
    }

    atomic_set(&library_terminating, 0);

    pdebug(DEBUG_INFO,"Done.");
}


int plc_tag_tickler_wake_impl(const char *func, int line_num)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_DETAIL, "Starting. Called from %s:%d.", func, line_num);

    if(!tag_tickler_wait) {
        pdebug(DEBUG_WARN, "Called from %s:%d when tag tickler condition var is NULL!", func, line_num);
        return PLCTAG_ERR_NULL_PTR;
    }

    rc = cond_signal(tag_tickler_wait);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Error %s trying to signal condition variable in call from %s:%d", plc_tag_decode_error(rc), func, line_num);
        return rc;
    }

    pdebug(DEBUG_DETAIL, "Done. Called from %s:%d.", func, line_num);

    return rc;
}



int plc_tag_generic_wake_tag_impl(const char *func, int line_num, plc_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_DETAIL, "Starting. Called from %s:%d.", func, line_num);

    if(!tag) {
        pdebug(DEBUG_WARN, "Called from %s:%d when tag is NULL!", func, line_num);
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!tag->tag_cond_wait) {
        pdebug(DEBUG_WARN, "Called from %s:%d when tag condition var is NULL!", func, line_num);
        return PLCTAG_ERR_NULL_PTR;
    }

    rc = cond_signal(tag->tag_cond_wait);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Error %s trying to signal condition variable in call from %s:%d", plc_tag_decode_error(rc), func, line_num);
        return rc;
    }

    pdebug(DEBUG_DETAIL, "Done. Called from %s:%d.", func, line_num);

    return rc;
}



/*
 * plc_tag_generic_tickler
 *
 * This implements the protocol-independent tickling functions such as handling
 * automatic tag operations and callbacks.
 */

void plc_tag_generic_tickler(plc_tag_p tag)
{
    if(tag) {
        debug_set_tag_id(tag->tag_id);

        pdebug(DEBUG_DETAIL, "Tickling tag %d.", tag->tag_id);

        /* if this tag has automatic writes, then there are many things we should check */
        if(tag->auto_sync_write_ms > 0) {
            /* has the tag been written to? */
            if(tag->tag_is_dirty) {
                /* abort any in flight read if the tag is dirty. */
                if(tag->read_in_flight) {
                    if(tag->vtable->abort) {
                        tag->vtable->abort(tag);
                    }

                    pdebug(DEBUG_DETAIL, "Aborting in-flight automatic read!");

                    tag->read_complete = 0;
                    tag->read_in_flight = 0;

                    /* TODO - should we report an ABORT event here? */
                    //tag->event_operation_aborted = 1;
                    tag_raise_event(tag, PLCTAG_EVENT_ABORTED, PLCTAG_ERR_ABORT);
                }

                /* have we already done something about automatic reads? */
                if(!tag->auto_sync_next_write) {
                    /* we need to queue up a new write. */
                    tag->auto_sync_next_write = time_ms() + tag->auto_sync_write_ms;

                    pdebug(DEBUG_DETAIL, "Queueing up automatic write in %dms.", tag->auto_sync_write_ms);
                } else if(!tag->write_in_flight && tag->auto_sync_next_write <= time_ms()) {
                    pdebug(DEBUG_DETAIL, "Triggering automatic write start.");

                    /* clear out any outstanding reads. */
                    if(tag->read_in_flight && tag->vtable->abort) {
                        tag->vtable->abort(tag);
                        tag->read_in_flight = 0;
                    }

                    tag->tag_is_dirty = 0;
                    tag->write_in_flight = 1;
                    tag->auto_sync_next_write = 0;

                    if(tag->vtable->write) {
                        tag->status = (int8_t)tag->vtable->write(tag);
                    }

                    // tag->event_write_started = 1;
                    tag_raise_event(tag, PLCTAG_EVENT_WRITE_STARTED, tag->status);
                }
            }
        }

        /* if this tag has automatic reads, we need to check that state too. */
        if(tag->auto_sync_read_ms > 0) {
            int64_t current_time = time_ms();

            // /* spread these out randomly to avoid too much clustering. */
            // if(tag->auto_sync_next_read == 0) {
            //     tag->auto_sync_next_read = current_time - (rand() % tag->auto_sync_read_ms);
            // }

            /* do we need to read? */
            if(tag->auto_sync_next_read < current_time) {
                /* make sure that we do not have an outstanding read or write. */
                if(!tag->read_in_flight && !tag->tag_is_dirty && !tag->write_in_flight) {
                    int64_t periods = 0;

                    pdebug(DEBUG_DETAIL, "Triggering automatic read start.");

                    tag->read_in_flight = 1;

                    if(tag->vtable->read) {
                        tag->status = (int8_t)tag->vtable->read(tag);
                    }

                    // tag->event_read_started = 1;
                    tag_raise_event(tag, PLCTAG_EVENT_READ_STARTED, tag->status);

                    /*
                    * schedule the next read.
                    *
                    * Note that there will be some jitter.  In that case we want to skip
                    * to the next read time that is a whole multiple of the read period.
                    *
                    * This keeps the jitter from slowly moving the polling cycle.
                    *
                    * Round up to the next period.
                    */
                    periods = (current_time - tag->auto_sync_next_read + (tag->auto_sync_read_ms - 1))/tag->auto_sync_read_ms;

                    /* warn if we need to skip more than one period. */
                    if(periods > 1) {
                        pdebug(DEBUG_WARN, "Skipping %" PRId64 " periods of %" PRId32 "ms.", periods, tag->auto_sync_read_ms);
                    }

                    tag->auto_sync_next_read += (periods * tag->auto_sync_read_ms);
                    pdebug(DEBUG_DETAIL, "Scheduling next read at time %" PRId64 ".", tag->auto_sync_next_read);
                } else {
                    pdebug(DEBUG_SPEW, "Unable to start read tag->read_in_flight=%d, tag->tag_is_dirty=%d, tag->write_in_flight=%d!", tag->read_in_flight, tag->tag_is_dirty, tag->write_in_flight);
                }
            }
        }
    } else {
        pdebug(DEBUG_WARN, "Called with null tag pointer!");
    }

    pdebug(DEBUG_DETAIL, "Done.");

    debug_set_tag_id(0);
}



void plc_tag_generic_handle_event_callbacks(plc_tag_p tag)
{
    critical_block(tag->api_mutex) {
        /* call the callbacks outside the API mutex. */
        if(tag && tag->callback) {
            debug_set_tag_id(tag->tag_id);

            /* trigger this if there is any other event. Only once. */
            if(tag->event_creation_complete) {
                pdebug(DEBUG_DETAIL, "Tag creation complete with status %s.", plc_tag_decode_error(tag->event_creation_complete_status));
                tag->callback(tag->tag_id, PLCTAG_EVENT_CREATED, tag->event_creation_complete_status, tag->userdata);
                tag->event_creation_complete = 0;
                tag->event_creation_complete_status = PLCTAG_STATUS_OK;
            }

            /* was there a read start? */
            if(tag->event_read_started) {
                pdebug(DEBUG_DETAIL, "Tag read started with status %s.", plc_tag_decode_error(tag->event_read_started_status));
                tag->callback(tag->tag_id, PLCTAG_EVENT_READ_STARTED, tag->event_read_started_status, tag->userdata);
                tag->event_read_started = 0;
                tag->event_read_started_status = PLCTAG_STATUS_OK;
            }

            /* was there a write start? */
            if(tag->event_write_started) {
                pdebug(DEBUG_DETAIL, "Tag write started with status %s.", plc_tag_decode_error(tag->event_write_started_status));
                tag->callback(tag->tag_id, PLCTAG_EVENT_WRITE_STARTED, tag->event_write_started_status, tag->userdata);
                tag->event_write_started = 0;
                tag->event_write_started_status = PLCTAG_STATUS_OK;
            }

            /* was there an abort? */
            if(tag->event_operation_aborted) {
                pdebug(DEBUG_DETAIL, "Tag operation aborted with status %s.", plc_tag_decode_error(tag->event_operation_aborted_status));
                tag->callback(tag->tag_id, PLCTAG_EVENT_ABORTED, tag->event_operation_aborted_status, tag->userdata);
                tag->event_operation_aborted = 0;
                tag->event_operation_aborted_status = PLCTAG_STATUS_OK;
            }

            /* was there a read completion? */
            if(tag->event_read_complete) {
                pdebug(DEBUG_DETAIL, "Tag read completed with status %s.", plc_tag_decode_error(tag->event_read_complete_status));
                tag->callback(tag->tag_id, PLCTAG_EVENT_READ_COMPLETED, tag->event_read_complete_status, tag->userdata);
                tag->event_read_complete = 0;
                tag->event_read_complete_status = PLCTAG_STATUS_OK;
            }

            /* was there a write completion? */
            if(tag->event_write_complete) {
                pdebug(DEBUG_DETAIL, "Tag write completed with status %s.", plc_tag_decode_error(tag->event_write_complete_status));
                tag->callback(tag->tag_id, PLCTAG_EVENT_WRITE_COMPLETED, tag->event_write_complete_status, tag->userdata);
                tag->event_write_complete = 0;
                tag->event_write_complete_status = PLCTAG_STATUS_OK;
            }

            /* do this last so that we raise all other events first. we only start deletion events. */
            if(tag->event_deletion_started) {
                pdebug(DEBUG_DETAIL, "Tag deletion started with status %s.", plc_tag_decode_error(tag->event_creation_complete_status));
                tag->callback(tag->tag_id, PLCTAG_EVENT_DESTROYED, tag->event_deletion_started_status, tag->userdata);
                tag->event_deletion_started = 0;
                tag->event_deletion_started_status = PLCTAG_STATUS_OK;
            }

            debug_set_tag_id(0);
        }
    } /* end of API mutex critical area. */
}



int plc_tag_generic_init_tag(plc_tag_p tag, attr attribs, void (*tag_callback_func)(int32_t tag_id, int event, int status, void *userdata), void *userdata)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    /* get the connection group ID here rather than in each PLC specific tag type. */
    tag->connection_group_id = attr_get_int(attribs, "connection_group_id", 0);
    if(tag->connection_group_id < 0 || tag->connection_group_id > 32767) {
        pdebug(DEBUG_WARN, "Connection group ID must be between 0 and 32767, inclusive, but was %d!", tag->connection_group_id);
        return PLCTAG_ERR_OUT_OF_BOUNDS;
    }

    rc = mutex_create(&(tag->ext_mutex));
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to create tag external mutex!");
        return PLCTAG_ERR_CREATE;
    }

    rc = mutex_create(&(tag->api_mutex));
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to create tag API mutex!");
        return PLCTAG_ERR_CREATE;
    }

    rc = cond_create(&(tag->tag_cond_wait));
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to create tag condition variable!");
        return PLCTAG_ERR_CREATE;
    }

    /* do this early so that events can be raised early. */
    tag->callback = tag_callback_func;
    tag->userdata = userdata;

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}





THREAD_FUNC(tag_tickler_func)
{
    (void)arg;

    debug_set_tag_id(0);

    pdebug(DEBUG_INFO, "Starting.");

    while(!atomic_get(&library_terminating)) {
        int max_index = 0;
        int64_t timeout_wait_ms = TAG_TICKLER_TIMEOUT_MS;

        /* what is the maximum time we will wait until */
        tag_tickler_wait_timeout_end = time_ms() + timeout_wait_ms;

        critical_block(tag_lookup_mutex) {
            max_index = hashtable_capacity(tags);
        }

        for(int i=0; i < max_index; i++) {
            plc_tag_p tag = NULL;

            critical_block(tag_lookup_mutex) {
                /* look up the max index again. it may have changed. */
                max_index = hashtable_capacity(tags);

                if(i < max_index) {
                    tag = (plc_tag_p)hashtable_get_index(tags, i);

                    if(tag) {
                        debug_set_tag_id(tag->tag_id);
                        tag = (plc_tag_p)rc_inc(tag);
                    }
                } else {
                    debug_set_tag_id(0);
                    tag = NULL;
                }
            }

            if(tag) {
                debug_set_tag_id(tag->tag_id);

                if(!tag->skip_tickler) {
                    pdebug(DEBUG_DETAIL, "Tickling tag %d.", tag->tag_id);

                    /* try to hold the tag API mutex while all this goes on. */
                    if(mutex_try_lock(tag->api_mutex) == PLCTAG_STATUS_OK) {
                        plc_tag_generic_tickler(tag);

                        /* call the tickler function if we can. */
                        if(tag->vtable->tickler) {
                            /* call the tickler on the tag. */
                            tag->vtable->tickler(tag);

                            if(tag->read_complete) {
                                tag->read_complete = 0;
                                tag->read_in_flight = 0;

                                //tag->event_read_complete = 1;
                                tag_raise_event(tag, PLCTAG_EVENT_READ_COMPLETED, tag->status);

                                /* wake immediately */
                                plc_tag_tickler_wake();
                                cond_signal(tag->tag_cond_wait);
                            }

                            if(tag->write_complete) {
                                tag->write_complete = 0;
                                tag->write_in_flight = 0;
                                tag->auto_sync_next_write = 0;

                                // tag->event_write_complete = 1;
                                tag_raise_event(tag, PLCTAG_EVENT_WRITE_COMPLETED, tag->status);

                                /* wake immediately */
                                plc_tag_tickler_wake();
                                cond_signal(tag->tag_cond_wait);
                            }
                        }

                        /* wake up earlier if the time until the next write wake up is sooner. */
                        if(tag->auto_sync_next_write && tag->auto_sync_next_write < tag_tickler_wait_timeout_end) {
                            tag_tickler_wait_timeout_end = tag->auto_sync_next_write;
                        }

                        /* wake up earlier if the time until the next read wake up is sooner. */
                        if(tag->auto_sync_next_read && tag->auto_sync_next_read < tag_tickler_wait_timeout_end) {
                            tag_tickler_wait_timeout_end = tag->auto_sync_next_read;
                        }

                        /* we are done with the tag API mutex now. */
                        mutex_unlock(tag->api_mutex);

                        /* call callbacks */
                        plc_tag_generic_handle_event_callbacks(tag);
                    } else {
                        pdebug(DEBUG_DETAIL, "Skipping tag as it is already locked.");
                    }

                } else {
                    pdebug(DEBUG_DETAIL, "Tag has its own tickler.");
                }

                // pdebug(DEBUG_DETAIL, "Current time %" PRId64 ".", time_ms());
                // pdebug(DEBUG_DETAIL, "Time to wake %" PRId64 ".", tag_tickler_wait_timeout_end);
                // pdebug(DEBUG_DETAIL, "Auto read time %" PRId64 ".", tag->auto_sync_next_read);
                // pdebug(DEBUG_DETAIL, "Auto write time %" PRId64 ".", tag->auto_sync_next_write);

                debug_set_tag_id(0);
            }

            if(tag) {
                rc_dec(tag);
            }

            debug_set_tag_id(0);
        }

        if(tag_tickler_wait) {
            int64_t time_to_wait = tag_tickler_wait_timeout_end - time_ms();
            int wait_rc = PLCTAG_STATUS_OK;

            if(time_to_wait < TAG_TICKLER_TIMEOUT_MIN_MS) {
                time_to_wait = TAG_TICKLER_TIMEOUT_MIN_MS;
            }

            if(time_to_wait > 0) {
                wait_rc = cond_wait(tag_tickler_wait, (int)time_to_wait);
                if(wait_rc == PLCTAG_ERR_TIMEOUT) {
                    pdebug(DEBUG_DETAIL, "Tag tickler thread timed out waiting for something to do.");
                }
            } else {
                pdebug(DEBUG_DETAIL, "Not waiting as time to wake is in the past.");
            }
        }
    }

    debug_set_tag_id(0);

    pdebug(DEBUG_INFO,"Terminating.");

    THREAD_RETURN(0);
}


/**************************************************************************
 ***************************  API Functions  ******************************
 **************************************************************************/


/*
 * plc_tag_decode_error()
 *
 * This takes an integer error value and turns it into a printable string.
 *
 * TODO - this should produce better errors than this!
 */



LIB_EXPORT const char *plc_tag_decode_error(int rc)
{
    switch(rc) {
    case PLCTAG_STATUS_PENDING:
        return "PLCTAG_STATUS_PENDING";
    case PLCTAG_STATUS_OK:
        return "PLCTAG_STATUS_OK";
    case PLCTAG_ERR_ABORT:
        return "PLCTAG_ERR_ABORT";
    case PLCTAG_ERR_BAD_CONFIG:
        return "PLCTAG_ERR_BAD_CONFIG";
    case PLCTAG_ERR_BAD_CONNECTION:
        return "PLCTAG_ERR_BAD_CONNECTION";
    case PLCTAG_ERR_BAD_DATA:
        return "PLCTAG_ERR_BAD_DATA";
    case PLCTAG_ERR_BAD_DEVICE:
        return "PLCTAG_ERR_BAD_DEVICE";
    case PLCTAG_ERR_BAD_GATEWAY:
        return "PLCTAG_ERR_BAD_GATEWAY";
    case PLCTAG_ERR_BAD_PARAM:
        return "PLCTAG_ERR_BAD_PARAM";
    case PLCTAG_ERR_BAD_REPLY:
        return "PLCTAG_ERR_BAD_REPLY";
    case PLCTAG_ERR_BAD_STATUS:
        return "PLCTAG_ERR_BAD_STATUS";
    case PLCTAG_ERR_CLOSE:
        return "PLCTAG_ERR_CLOSE";
    case PLCTAG_ERR_CREATE:
        return "PLCTAG_ERR_CREATE";
    case PLCTAG_ERR_DUPLICATE:
        return "PLCTAG_ERR_DUPLICATE";
    case PLCTAG_ERR_ENCODE:
        return "PLCTAG_ERR_ENCODE";
    case PLCTAG_ERR_MUTEX_DESTROY:
        return "PLCTAG_ERR_MUTEX_DESTROY";
    case PLCTAG_ERR_MUTEX_INIT:
        return "PLCTAG_ERR_MUTEX_INIT";
    case PLCTAG_ERR_MUTEX_LOCK:
        return "PLCTAG_ERR_MUTEX_LOCK";
    case PLCTAG_ERR_MUTEX_UNLOCK:
        return "PLCTAG_ERR_MUTEX_UNLOCK";
    case PLCTAG_ERR_NOT_ALLOWED:
        return "PLCTAG_ERR_NOT_ALLOWED";
    case PLCTAG_ERR_NOT_FOUND:
        return "PLCTAG_ERR_NOT_FOUND";
    case PLCTAG_ERR_NOT_IMPLEMENTED:
        return "PLCTAG_ERR_NOT_IMPLEMENTED";
    case PLCTAG_ERR_NO_DATA:
        return "PLCTAG_ERR_NO_DATA";
    case PLCTAG_ERR_NO_MATCH:
        return "PLCTAG_ERR_NO_MATCH";
    case PLCTAG_ERR_NO_MEM:
        return "PLCTAG_ERR_NO_MEM";
    case PLCTAG_ERR_NO_RESOURCES:
        return "PLCTAG_ERR_NO_RESOURCES";
    case PLCTAG_ERR_NULL_PTR:
        return "PLCTAG_ERR_NULL_PTR";
    case PLCTAG_ERR_OPEN:
        return "PLCTAG_ERR_OPEN";
    case PLCTAG_ERR_OUT_OF_BOUNDS:
        return "PLCTAG_ERR_OUT_OF_BOUNDS";
    case PLCTAG_ERR_READ:
        return "PLCTAG_ERR_READ";
    case PLCTAG_ERR_REMOTE_ERR:
        return "PLCTAG_ERR_REMOTE_ERR";
    case PLCTAG_ERR_THREAD_CREATE:
        return "PLCTAG_ERR_THREAD_CREATE";
    case PLCTAG_ERR_THREAD_JOIN:
        return "PLCTAG_ERR_THREAD_JOIN";
    case PLCTAG_ERR_TIMEOUT:
        return "PLCTAG_ERR_TIMEOUT";
    case PLCTAG_ERR_TOO_LARGE:
        return "PLCTAG_ERR_TOO_LARGE";
    case PLCTAG_ERR_TOO_SMALL:
        return "PLCTAG_ERR_TOO_SMALL";
    case PLCTAG_ERR_UNSUPPORTED:
        return "PLCTAG_ERR_UNSUPPORTED";
    case PLCTAG_ERR_WINSOCK:
        return "PLCTAG_ERR_WINSOCK";
    case PLCTAG_ERR_WRITE:
        return "PLCTAG_ERR_WRITE";
    case PLCTAG_ERR_PARTIAL:
        return "PLCTAG_ERR_PARTIAL";
    case PLCTAG_ERR_BUSY:
        return "PLCTAG_ERR_BUSY";

    default:
        return "Unknown error.";
        break;
    }

    return "Unknown error.";
}





/*
 * Set the debug level.
 *
 * This function takes values from the defined debug levels.  It sets
 * the debug level to the passed value.  Higher numbers output increasing amounts
 * of information.   Input values not defined will be ignored.
 */

LIB_EXPORT void plc_tag_set_debug_level(int debug_level)
{
	if (debug_level >= PLCTAG_DEBUG_NONE && debug_level <= PLCTAG_DEBUG_SPEW) {
		set_debug_level(debug_level);
	}
}




/*
 * Check that the library supports the required API version.
 *
 * PLCTAG_STATUS_OK is returned if the version matches.  If it does not,
 * PLCTAG_ERR_UNSUPPORTED is returned.
 */

LIB_EXPORT int plc_tag_check_lib_version(int req_major, int req_minor, int req_patch)
{
    /* encode these with 16-bits per version part. */
    uint64_t lib_encoded_version = (((uint64_t)version_major) << 32u)
                                 + (((uint64_t)version_minor) << 16u)
                                   + (uint64_t)version_patch;

    uint64_t req_encoded_version = (((uint64_t)req_major) << 32u)
                                 + (((uint64_t)req_minor) << 16u)
                                   + (uint64_t)req_patch;

    if(version_major == (uint64_t)req_major && lib_encoded_version >= req_encoded_version) {
        return PLCTAG_STATUS_OK;
    } else {
        return PLCTAG_ERR_UNSUPPORTED;
    }
}






/*
 * plc_tag_create()
 *
 * This is where the dispatch occurs to the protocol specific implementation.
 */

LIB_EXPORT int32_t plc_tag_create(const char *attrib_str, int timeout)
{
    return plc_tag_create_ex(attrib_str, NULL, NULL, timeout);
}


LIB_EXPORT int32_t plc_tag_create_ex(const char *attrib_str, void (*tag_callback_func)(int32_t tag_id, int event, int status, void *userdata), void *userdata, int timeout)
{
    plc_tag_p tag = PLC_TAG_P_NULL;
    int id = PLCTAG_ERR_OUT_OF_BOUNDS;
    attr attribs = NULL;
    int rc = PLCTAG_STATUS_OK;
    int read_cache_ms = 0;
    tag_create_function tag_constructor;
	int debug_level = -1;

    /* we are creating a tag, there is no ID yet. */
    debug_set_tag_id(0);

    pdebug(DEBUG_INFO,"Starting");

    /* check to see if the library is terminating. */
    if(atomic_get(&library_terminating)) {
        pdebug(DEBUG_WARN, "The plctag library is in the process of shutting down!");
        return PLCTAG_ERR_NOT_ALLOWED;
    }

    /* make sure that all modules are initialized. */
    if((rc = initialize_modules()) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR,"Unable to initialize the internal library state!");
        return rc;
    }

    /* check the arguments */

    if(timeout < 0) {
        pdebug(DEBUG_WARN, "Timeout must not be negative!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    if(!attrib_str || str_length(attrib_str) == 0) {
        pdebug(DEBUG_WARN,"Tag attribute string is null or zero length!");
        return PLCTAG_ERR_TOO_SMALL;
    }

    attribs = attr_create_from_str(attrib_str);
    if(!attribs) {
        pdebug(DEBUG_WARN,"Unable to parse attribute string!");
        return PLCTAG_ERR_BAD_DATA;
    }

    /* set debug level */
	debug_level = attr_get_int(attribs, "debug", -1);
	if (debug_level > DEBUG_NONE) {
		set_debug_level(debug_level);
	}

    /*
     * create the tag, this is protocol specific.
     *
     * If this routine wants to keep the attributes around, it needs
     * to clone them.
     */
    tag_constructor = find_tag_create_func(attribs);

    if(!tag_constructor) {
        pdebug(DEBUG_WARN,"Tag creation failed, no tag constructor found for tag type!");
        attr_destroy(attribs);
        return PLCTAG_ERR_BAD_PARAM;
    }

    tag = tag_constructor(attribs, tag_callback_func, userdata);

    if(!tag) {
        pdebug(DEBUG_WARN, "Tag creation failed, skipping mutex creation and other generic setup.");
        attr_destroy(attribs);
        return PLCTAG_ERR_CREATE;
    }

    if(tag->status != PLCTAG_STATUS_OK && tag->status != PLCTAG_STATUS_PENDING) {
        int tag_status = tag->status;

        pdebug(DEBUG_WARN, "Warning, %s error found while creating tag!", plc_tag_decode_error(tag_status));

        attr_destroy(attribs);
        rc_dec(tag);

        return tag_status;
    }

    /* set up the read cache config. */
    read_cache_ms = attr_get_int(attribs,"read_cache_ms",0);
    if(read_cache_ms < 0) {
        pdebug(DEBUG_WARN, "read_cache_ms value must be positive, using zero.");
        read_cache_ms = 0;
    }

    tag->read_cache_expire = (int64_t)0;
    tag->read_cache_ms = (int64_t)read_cache_ms;

    /* set up any automatic read/write */
    tag->auto_sync_read_ms = attr_get_int(attribs, "auto_sync_read_ms", 0);
    if(tag->auto_sync_read_ms < 0) {
        pdebug(DEBUG_WARN, "auto_sync_read_ms value must be positive!");
        attr_destroy(attribs);
        rc_dec(tag);
        return PLCTAG_ERR_BAD_PARAM;
    } else if(tag->auto_sync_read_ms > 0) {
        /* how many periods did we already pass? */
        // int64_t periods = (time_ms() / tag->auto_sync_read_ms);
        // tag->auto_sync_next_read = (periods + 1) * tag->auto_sync_read_ms;
        /* start some time in the future, but with random jitter. */
        tag->auto_sync_next_read = time_ms() + (rand() % tag->auto_sync_read_ms);
    }

    tag->auto_sync_write_ms = attr_get_int(attribs, "auto_sync_write_ms", 0);
    if(tag->auto_sync_write_ms < 0) {
        pdebug(DEBUG_WARN, "auto_sync_write_ms value must be positive!");
        attr_destroy(attribs);
        rc_dec(tag);
        return PLCTAG_ERR_BAD_PARAM;
    } else {
        tag->auto_sync_next_write = 0;
    }

    /* set up the tag byte order if there are any overrides. */
    rc = set_tag_byte_order(tag, attribs);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to correctly set tag data byte order: %s!", plc_tag_decode_error(rc));
        attr_destroy(attribs);
        rc_dec(tag);
        return rc;
    }

    /*
     * Release memory for attributes
     */
    attr_destroy(attribs);

    /* map the tag to a tag ID */
    id = add_tag_lookup(tag);

    /* if the mapping failed, then punt */
    if(id < 0) {
        pdebug(DEBUG_ERROR, "Unable to map tag %p to lookup table entry, rc=%s", tag, plc_tag_decode_error(id));
        rc_dec(tag);
        return id;
    }

    /* save this for later. */
    tag->tag_id = id;

    debug_set_tag_id(id);

    pdebug(DEBUG_INFO, "Returning mapped tag ID %d", id);

    /* wake up tag's PLC here. */
    if(tag->vtable->wake_plc) {
        tag->vtable->wake_plc(tag);
    }

    /* get the tag status. */
    rc = tag->vtable->status(tag);

    /* check to see if there was an error during tag creation. */
    if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
        pdebug(DEBUG_WARN, "Error %s while trying to create tag!", plc_tag_decode_error(rc));
        if(tag->vtable->abort) {
            tag->vtable->abort(tag);
        }

        /* remove the tag from the hashtable. */
        critical_block(tag_lookup_mutex) {
            hashtable_remove(tags, (int64_t)tag->tag_id);
        }

        rc_dec(tag);
        return rc;
    }

    pdebug(DEBUG_DETAIL, "Tag status after creation is %s.", plc_tag_decode_error(rc));

    /*
    * if there is a timeout, then wait until we get
    * an error or we timeout.
    */
    if(timeout > 0 && rc == PLCTAG_STATUS_PENDING) {
        int64_t start_time = time_ms();
        int64_t end_time = start_time + timeout;

        /* wake up the tickler in case it is needed to create the tag. */
        plc_tag_tickler_wake();

        /* we loop as long as we have time left to wait. */
        do {
            int64_t timeout_left = end_time - time_ms();

            /* clamp the timeout left to non-negative int range. */
            if(timeout_left < 0) {
                timeout_left = 0;
            }

            if(timeout_left > INT_MAX) {
                timeout_left = 100; /* MAGIC, only wait 100ms in this weird case. */
            }

            /* wait for something to happen */
            rc = cond_wait(tag->tag_cond_wait, (int)timeout_left);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Error %s while waiting for tag creation to complete!", plc_tag_decode_error(rc));
                if(tag->vtable->abort) {
                    tag->vtable->abort(tag);
                }

                /* remove the tag from the hashtable. */
                critical_block(tag_lookup_mutex) {
                    hashtable_remove(tags, (int64_t)tag->tag_id);
                }

                rc_dec(tag);
                return rc;
            }

            /* get the tag status. */
            rc = tag->vtable->status(tag);

            /* check to see if there was an error during tag creation. */
            if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
                pdebug(DEBUG_WARN, "Error %s while trying to create tag!", plc_tag_decode_error(rc));
                if(tag->vtable->abort) {
                    tag->vtable->abort(tag);
                }

                /* remove the tag from the hashtable. */
                critical_block(tag_lookup_mutex) {
                    hashtable_remove(tags, (int64_t)tag->tag_id);
                }

                rc_dec(tag);
                return rc;
            }
        } while(rc == PLCTAG_STATUS_PENDING && time_ms() > end_time);

        /* clear up any remaining flags.  This should be refactored. */
        tag->read_in_flight = 0;
        tag->write_in_flight = 0;

        /* raise create event. */
        tag_raise_event(tag, PLCTAG_EVENT_CREATED, (int8_t)rc);

        pdebug(DEBUG_INFO,"tag set up elapsed time %" PRId64 "ms",(time_ms()-start_time));
    }

    /* dispatch any outstanding events. */
    plc_tag_generic_handle_event_callbacks(tag);

    pdebug(DEBUG_INFO,"Done.");

    return id;
}




/*
 * plc_tag_shutdown
 *
 * Some systems may not be able to call atexit() handlers.  In those cases, wrappers should
 * call this function before unloading the library or terminating.   Most OSes will cleanly
 * recover all system resources when a process is terminated and this will not be necessary.
 */

LIB_EXPORT void plc_tag_shutdown(void)
{
    int tag_table_entries = 0;

    pdebug(DEBUG_INFO, "Starting.");

    /* terminate anything waiting on the library and prevent any tags from being created. */
    atomic_set(&library_terminating, 1);

    /* close all tags. */
    pdebug(DEBUG_DETAIL, "Closing all tags.");

    critical_block(tag_lookup_mutex) {
        tag_table_entries = hashtable_capacity(tags);
    }

    for(int i=0; i<tag_table_entries; i++) {
        plc_tag_p tag = NULL;

        critical_block(tag_lookup_mutex) {
            tag_table_entries = hashtable_capacity(tags);

            if(i<tag_table_entries && tag_table_entries >= 0) {
                tag = (plc_tag_p)hashtable_get_index(tags, i);

                /* make sure the tag does not go away while we are using the pointer. */
                if(tag) {
                    /* this returns NULL if the existing ref-count is zero. */
                    tag = (plc_tag_p)rc_inc(tag);
                }
            }
        }

        /* do this outside the mutex. */
        if(tag) {
            debug_set_tag_id(tag->tag_id);
            pdebug(DEBUG_DETAIL, "Destroying tag %" PRId32 ".", tag->tag_id);
            plc_tag_destroy(tag->tag_id);
            rc_dec(tag);
        }
    }

    pdebug(DEBUG_DETAIL, "All tags closed.");

    pdebug(DEBUG_DETAIL, "Cleaning up library resources.");

    destroy_modules();

    /* Clear the termination flag in case we want to start up again. */
    atomic_set(&library_terminating, 0);

    pdebug(DEBUG_INFO, "Done.");
}



/*
 * plc_tag_register_callback
 *
 * This function registers the passed callback function with the tag.  Only one callback function
 * may be registered on a tag at a time!
 *
 * Once registered, any of the following operations on or in the tag will result in the callback
 * being called:
 *
 *      * a tag handle finishing creation.
 *      * starting a tag read operation.
 *      * a tag read operation ending.
 *      * a tag read being aborted.
 *      * starting a tag write operation.
 *      * a tag write operation ending.
 *      * a tag write being aborted.
 *      * a tag being destroyed
 *
 * The callback is called outside of the internal tag mutex so it can call any tag functions safely.   However,
 * the callback is called in the context of the internal tag helper thread and not the client library thread(s).
 * This means that YOU are responsible for making sure that all client application data structures the callback
 * function touches are safe to access by the callback!
 *
 * Do not do any operations in the callback that block for any significant time.   This will cause library
 * performance to be poor or even to start failing!
 *
 * When the callback is called with the PLCTAG_EVENT_DESTROY_STARTED, do not call any tag functions.  It is
 * not guaranteed that they will work and they will possibly hang or fail.
 *
 * Return values:
 *void (*tag_callback_func)(int32_t tag_id, uint32_t event, int status)
 * If there is already a callback registered, the function will return PLCTAG_ERR_DUPLICATE.   Only one callback
 * function may be registered at a time on each tag.
 *
 * If all is successful, the function will return PLCTAG_STATUS_OK.
 *
 * Also see plc_tag_register_callback_ex.
 */

LIB_EXPORT int plc_tag_register_callback(int32_t tag_id, tag_callback_func callback_func)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    rc = plc_tag_register_callback_ex(tag_id, (tag_extended_callback_func)callback_func, NULL);

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}



/*
 * plc_tag_register_callback_ex
 *
 * This function registers the passed callback function and user data with the tag.  Only one callback function
 * may be registered on a tag at a time!
 *
 * Once registered, any of the following operations on or in the tag will result in the callback
 * being called:
 *
 *      * a tag handle finishing creation.
 *      * starting a tag read operation.
 *      * a tag read operation ending.
 *      * a tag read being aborted.
 *      * starting a tag write operation.
 *      * a tag write operation ending.
 *      * a tag write being aborted.
 *      * a tag being destroyed
 *
 * The callback is called outside of the internal tag mutex so it can call any tag functions safely.   However,
 * the callback is called in the context of the internal tag helper thread and not the client library thread(s).
 * This means that YOU are responsible for making sure that all client application data structures the callback
 * function touches are safe to access by the callback!
 *
 * Do not do any operations in the callback that block for any significant time.   This will cause library
 * performance to be poor or even to start failing!
 *
 * When the callback is called with the PLCTAG_EVENT_DESTROY_STARTED, do not call any tag functions.  It is
 * not guaranteed that they will work and they will possibly hang or fail.
 *
 * Return values:
 *
 * If there is already a callback registered, the function will return PLCTAG_ERR_DUPLICATE.   Only one callback
 * function may be registered at a time on each tag.
 *
 * If all is successful, the function will return PLCTAG_STATUS_OK.
 *
 * Also see plc_tag_register_callback.
 */

LIB_EXPORT int plc_tag_register_callback_ex(int32_t tag_id, tag_extended_callback_func callback_func, void *userdata)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(tag_id);

    pdebug(DEBUG_INFO, "Starting.");

    if (!tag) {
        pdebug(DEBUG_WARN, "Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    critical_block(tag->api_mutex) {
        if(tag->callback) {
            rc = PLCTAG_ERR_DUPLICATE;
        } else {
            if(callback_func) {
                tag->callback = callback_func;
                tag->userdata = userdata;
            } else {
                tag->callback = NULL;
                tag->userdata = NULL;
            }
        }
    }

    rc_dec(tag);

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}



/*
 * plc_tag_unregister_callback
 *
 * This function removes the callback already registered on the tag.
 *
 * Return values:
 *
 * The function returns PLCTAG_STATUS_OK if there was a registered callback and removing it went well.
 * An error of PLCTAG_ERR_NOT_FOUND is returned if there was no registered callback.
 */

LIB_EXPORT int plc_tag_unregister_callback(int32_t tag_id)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(tag_id);

    pdebug(DEBUG_INFO, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    critical_block(tag->api_mutex) {
        if(tag->callback) {
            rc = PLCTAG_STATUS_OK;
            tag->callback = NULL;
            tag->userdata = NULL;
        } else {
            rc = PLCTAG_ERR_NOT_FOUND;
        }
    }

    rc_dec(tag);

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}




/*
 * plc_tag_register_logger
 *
 * This function registers the passed callback function with the library.  Only one callback function
 * may be registered with the library at a time!
 *
 * Once registered, the function will be called with any logging message that is normally printed due
 * to the current log level setting.
 *
 * WARNING: the callback will usually be called when the internal tag API mutex is held.   You cannot
 * call any tag functions within the callback!
 *
 * Return values:
 *
 * If there is already a callback registered, the function will return PLCTAG_ERR_DUPLICATE.   Only one callback
 * function may be registered at a time on each tag.
 *
 * If all is successful, the function will return PLCTAG_STATUS_OK.
 */

LIB_EXPORT int plc_tag_register_logger(void (*log_callback_func)(int32_t tag_id, int debug_level, const char *message))
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_DETAIL, "Starting.");

    rc = debug_register_logger(log_callback_func);

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}



/*
 * plc_tag_unregister_logger
 *
 * This function removes the logger callback already registered for the library.
 *
 * Return values:
 *
 * The function returns PLCTAG_STATUS_OK if there was a registered callback and removing it went well.
 * An error of PLCTAG_ERR_NOT_FOUND is returned if there was no registered callback.
 */

LIB_EXPORT int plc_tag_unregister_logger(void)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_DETAIL, "Starting");

    rc = debug_unregister_logger();

    pdebug(DEBUG_DETAIL, "Done.");

    return rc;
}



/*
 * plc_tag_lock
 *
 * Lock the tag against use by other threads.  Because operations on a tag are
 * very much asynchronous, actions like getting and extracting the data from
 * a tag take more than one API call.  If more than one thread is using the same tag,
 * then the internal state of the tag will get broken and you will probably experience
 * a crash.
 *
 * This should be used to initially lock a tag when starting operations with it
 * followed by a call to plc_tag_unlock when you have everything you need from the tag.
 */

LIB_EXPORT int plc_tag_lock(int32_t id)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_INFO, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* we cannot nest the mutexes otherwise we will deadlock. */
    do {
        critical_block(tag->api_mutex) {
            rc = mutex_try_lock(tag->ext_mutex);
        }

        /* if the mutex is already locked then we get a mutex lock error. */
        if (rc == PLCTAG_ERR_MUTEX_LOCK) {
            pdebug(DEBUG_SPEW, "Mutex already locked, wait and retry.");
            sleep_ms(10);
        }
    } while(rc == PLCTAG_ERR_MUTEX_LOCK);

    if (rc == PLCTAG_STATUS_OK) {
        pdebug(DEBUG_SPEW, "External mutex locked.");
    }
    else {
        pdebug(DEBUG_WARN, "Error %s trying to lock external mutex!", plc_tag_decode_error(rc));
    }

    rc_dec(tag);

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}




/*
 * plc_tag_unlock
 *
 * The opposite action of plc_tag_unlock.  This allows other threads to access the
 * tag.
 */

LIB_EXPORT int plc_tag_unlock(int32_t id)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_INFO, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    critical_block(tag->api_mutex) {
        rc = mutex_unlock(tag->ext_mutex);
    }

    rc_dec(tag);

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}




/*
 * plc_tag_abort()
 *
 * This function calls through the vtable in the passed tag to call
 * the protocol-specific implementation.
 *
 * The implementation must do whatever is necessary to abort any
 * ongoing IO.
 *
 * The status of the operation is returned.
 */

LIB_EXPORT int plc_tag_abort(int32_t id)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_INFO, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    critical_block(tag->api_mutex) {
        /* who knows what state the tag data is in.  */
        tag->read_cache_expire = (uint64_t)0;

        if(!tag->vtable || !tag->vtable->abort) {
            pdebug(DEBUG_WARN,"Tag does not have a abort function!");
            rc = PLCTAG_ERR_NOT_IMPLEMENTED;
            break;
        }

        /* this may be synchronous. */
        rc = tag->vtable->abort(tag);

        tag->read_in_flight = 0;
        tag->read_complete = 0;
        tag->write_in_flight = 0;
        tag->write_complete = 0;

        tag_raise_event(tag, PLCTAG_EVENT_ABORTED, PLCTAG_ERR_ABORT);
    }

    /* release the kraken... or tickler */
    plc_tag_tickler_wake();

    plc_tag_generic_handle_event_callbacks(tag);

    rc_dec(tag);

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}




/*
 * plc_tag_destroy()
 *
 * Remove all implementation specific details about a tag and clear its
 * memory.
 */


LIB_EXPORT int plc_tag_destroy(int32_t tag_id)
{
    plc_tag_p tag = NULL;

    debug_set_tag_id((int)tag_id);

    pdebug(DEBUG_INFO, "Starting.");

    if(tag_id <= 0 || tag_id >= TAG_ID_MASK) {
        pdebug(DEBUG_WARN, "Called with zero or invalid tag!");
        return PLCTAG_ERR_NULL_PTR;
    }

    critical_block(tag_lookup_mutex) {
        tag = (plc_tag_p)hashtable_remove(tags, tag_id);
    }

    if(!tag) {
        pdebug(DEBUG_WARN, "Called with non-existent tag!");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* abort anything in flight */
    pdebug(DEBUG_DETAIL, "Aborting any in-flight operations.");

    critical_block(tag->api_mutex) {
        if(!tag->vtable || !tag->vtable->abort) {
            pdebug(DEBUG_WARN,"Tag does not have a abort function!");
        } else {
            /* Force a clean up. */
            tag->vtable->abort(tag);
        }

        tag_raise_event(tag, PLCTAG_EVENT_DESTROYED, PLCTAG_STATUS_OK);
    }

    /* wake the tickler */
    plc_tag_tickler_wake();

    plc_tag_generic_handle_event_callbacks(tag);

    /* release the reference outside the mutex. */
    rc_dec(tag);

    pdebug(DEBUG_INFO, "Done.");

    debug_set_tag_id(0);

    return PLCTAG_STATUS_OK;
}





/*
 * plc_tag_read()
 *
 * This function calls through the vtable in the passed tag to call
 * the protocol-specific implementation.  That starts the read operation.
 * If there is a timeout passed, then this routine waits for either
 * a timeout or an error.
 *
 * The status of the operation is returned.
 */

LIB_EXPORT int plc_tag_read(int32_t id, int timeout)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);
    int is_done = 0;

    pdebug(DEBUG_INFO, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    if(timeout < 0) {
        pdebug(DEBUG_WARN, "Timeout must not be negative!");
        rc_dec(tag);
        return PLCTAG_ERR_BAD_PARAM;
    }

    critical_block(tag->api_mutex) {
        tag_raise_event(tag, PLCTAG_EVENT_READ_STARTED, PLCTAG_STATUS_OK);
        plc_tag_generic_handle_event_callbacks(tag);

        /* check read cache, if not expired, return existing data. */
        if(tag->read_cache_expire > time_ms()) {
            pdebug(DEBUG_INFO, "Returning cached data.");
            rc = PLCTAG_STATUS_OK;
            is_done = 1;
            break;
        }

        if(tag->read_in_flight || tag->write_in_flight) {
            pdebug(DEBUG_WARN, "An operation is already in flight!");
            rc = PLCTAG_ERR_BUSY;
            is_done = 1;
            break;
        }

        if(tag->tag_is_dirty) {
            pdebug(DEBUG_WARN, "Tag has locally updated data that will be overwritten!");
            rc = PLCTAG_ERR_BUSY;
            is_done = 1;
            break;
        }

        tag->read_in_flight = 1;
        tag->status = PLCTAG_STATUS_PENDING;

        /* clear the condition var */
        cond_clear(tag->tag_cond_wait);

        /* the protocol implementation does not do the timeout. */
        rc = tag->vtable->read(tag);

        /* if not pending then check for success or error. */
        if(rc != PLCTAG_STATUS_PENDING) {
            if(rc != PLCTAG_STATUS_OK) {
                /* not pending and not OK, so error. Abort and clean up. */

                pdebug(DEBUG_WARN,"Response from read command returned error %s!", plc_tag_decode_error(rc));

                if(tag->vtable->abort) {
                    tag->vtable->abort(tag);
                }
            }

            tag->read_in_flight = 0;
            is_done = 1;
            break;
        }
    }

    /*
     * if there is a timeout, then wait until we get
     * an error or we timeout.
     */
    if(!is_done && timeout > 0) {
        int64_t start_time = time_ms();
        int64_t end_time = start_time + timeout;

        /* wake up the tickler in case it is needed to read the tag. */
        plc_tag_tickler_wake();

        /* we loop as long as we have time left to wait. */
        do {
            int64_t timeout_left = end_time - time_ms();

            /* clamp the timeout left to non-negative int range. */
            if(timeout_left < 0) {
                timeout_left = 0;
            }

            if(timeout_left > INT_MAX) {
                timeout_left = 100; /* MAGIC, only wait 100ms in this weird case. */
            }

            /* wait for something to happen */
            rc = cond_wait(tag->tag_cond_wait, (int)timeout_left);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Error %s while waiting for tag read to complete!", plc_tag_decode_error(rc));
                plc_tag_abort(id);

                break;
            }

            /* get the tag status. */
            rc = plc_tag_status(id);

            /* check to see if there was an error during tag read. */
            if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
                pdebug(DEBUG_WARN, "Error %s while trying to read tag!", plc_tag_decode_error(rc));
                plc_tag_abort(id);
            }
        } while(rc == PLCTAG_STATUS_PENDING && time_ms() < end_time);

        /* the read is not in flight anymore. */
        critical_block(tag->api_mutex) {
            tag->read_in_flight = 0;
            tag->read_complete = 0;
            is_done = 1;
            tag_raise_event(tag, PLCTAG_EVENT_READ_COMPLETED, (int8_t)rc);
        }

        pdebug(DEBUG_INFO,"elapsed time %" PRId64 "ms", (time_ms()-start_time));
    }

    if(rc == PLCTAG_STATUS_OK) {
        /* set up the cache time.  This works when read_cache_ms is zero as it is already expired. */
        tag->read_cache_expire = time_ms() + tag->read_cache_ms;
    }

    /* fire any events that are pending. */
    plc_tag_generic_handle_event_callbacks(tag);

    rc_dec(tag);

    pdebug(DEBUG_INFO, "Done");

    return rc;
}





/*
 * plc_tag_status
 *
 * Return the current status of the tag.  This will be PLCTAG_STATUS_PENDING if there is
 * an uncompleted IO operation.  It will be PLCTAG_STATUS_OK if everything is fine.  Other
 * errors will be returned as appropriate.
 *
 * This is a function provided by the underlying protocol implementation.
 */

LIB_EXPORT int plc_tag_status(int32_t id)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    /* check the ID.  It might be an error status from creating the tag. */
    if(!tag) {
        if(id < 0) {
            pdebug(DEBUG_WARN, "Called with an error status %s!", plc_tag_decode_error(id));
            return id;
        } else {
            pdebug(DEBUG_WARN,"Tag not found.");
            return PLCTAG_ERR_NOT_FOUND;
        }
    }

    critical_block(tag->api_mutex) {
        if(tag && tag->vtable->tickler) {
            tag->vtable->tickler(tag);
        }

        rc = tag->vtable->status(tag);

        if(rc == PLCTAG_STATUS_OK) {
            if(tag->read_in_flight || tag->write_in_flight) {
                rc = PLCTAG_STATUS_PENDING;
            }
        }
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done with rc=%s.", plc_tag_decode_error(rc));

    return rc;
}





/*
 * plc_tag_write()
 *
 * This function calls through the vtable in the passed tag to call
 * the protocol-specific implementation.  That starts the write operation.
 * If there is a timeout passed, then this routine waits for either
 * a timeout or an error.
 *
 * The status of the operation is returned.
 */

LIB_EXPORT int plc_tag_write(int32_t id, int timeout)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);
    int is_done = 0;

    pdebug(DEBUG_INFO, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    if(timeout < 0) {
        pdebug(DEBUG_WARN, "Timeout must not be negative!");
        rc_dec(tag);
        return PLCTAG_ERR_BAD_PARAM;
    }

    critical_block(tag->api_mutex) {
        if(tag->read_in_flight || tag->write_in_flight) {
            pdebug(DEBUG_WARN, "Tag already has an operation in flight!");
            is_done = 1;
            rc = PLCTAG_ERR_BUSY;
            break;
        }

        /* a write is now in flight. */
        tag->write_in_flight = 1;
        tag->status = PLCTAG_STATUS_OK;

        /*
         * This needs to be done before we raise the event below in case the user code
         * tries to do something tricky like abort the write.   In that case, the condition
         * variable will be set by the abort.   So we have to clear it here and then see
         * if it gets raised afterward.
         */
        cond_clear(tag->tag_cond_wait);

        /*
         * This must be raised _before_ we start the write to enable
         * application code to fill in the tag data buffer right before
         * we start the write process.
         */
        tag_raise_event(tag, PLCTAG_EVENT_WRITE_STARTED, tag->status);
        plc_tag_generic_handle_event_callbacks(tag);

        /* the protocol implementation does not do the timeout. */
        rc = tag->vtable->write(tag);

        /* if not pending then check for success or error. */
        if(rc != PLCTAG_STATUS_PENDING) {
            if(rc != PLCTAG_STATUS_OK) {
                /* not pending and not OK, so error. Abort and clean up. */

                pdebug(DEBUG_WARN,"Response from write command returned error %s!", plc_tag_decode_error(rc));

                if(tag->vtable->abort) {
                    tag->vtable->abort(tag);
                }
            }

            tag->write_in_flight = 0;
            is_done = 1;
            break;
        }
    } /* end of api mutex block */

    /*
     * if there is a timeout, then wait until we get
     * an error or we timeout.
     */
    if(!is_done && timeout > 0) {
        int64_t start_time = time_ms();
        int64_t end_time = start_time + timeout;

        /* wake up the tickler in case it is needed to write the tag. */
        plc_tag_tickler_wake();

        /* we loop as long as we have time left to wait. */
        do {
            int64_t timeout_left = end_time - time_ms();

            /* clamp the timeout left to non-negative int range. */
            if(timeout_left < 0) {
                timeout_left = 0;
            }

            if(timeout_left > INT_MAX) {
                timeout_left = 100; /* MAGIC, only wait 100ms in this weird case. */
            }

            /* wait for something to happen */
            rc = cond_wait(tag->tag_cond_wait, (int)timeout_left);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Error %s while waiting for tag write to complete!", plc_tag_decode_error(rc));
                plc_tag_abort(id);

                break;
            }

            /* get the tag status. */
            rc = plc_tag_status(id);

            /* check to see if there was an error during tag creation. */
            if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
                pdebug(DEBUG_WARN, "Error %s while trying to write tag!", plc_tag_decode_error(rc));
                plc_tag_abort(id);
            }
        } while(rc == PLCTAG_STATUS_PENDING && time_ms() < end_time);

        /* the write is not in flight anymore. */
        critical_block(tag->api_mutex) {
            tag->write_in_flight = 0;
            tag->write_complete = 0;
            is_done = 1;
        }

        pdebug(DEBUG_INFO,"Write finshed with elapsed time %" PRId64 "ms", (time_ms()-start_time));
    }

    if(is_done) {
        critical_block(tag->api_mutex) {
            tag_raise_event(tag, PLCTAG_EVENT_WRITE_COMPLETED, (int8_t)rc);
        }
    }

    /* fire any events that are pending. */
    plc_tag_generic_handle_event_callbacks(tag);

    rc_dec(tag);

    pdebug(DEBUG_INFO, "Done: status = %s.", plc_tag_decode_error(rc));

    return rc;
}





/*
 * Tag data accessors.
 */




LIB_EXPORT int plc_tag_get_int_attribute(int32_t id, const char *attrib_name, int default_value)
{
    int res = default_value;
    plc_tag_p tag = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    /* FIXME - this should set the tag status if there is a tag. */
    if(!attrib_name || str_length(attrib_name) == 0) {
        pdebug(DEBUG_WARN, "Attribute name must not be null or zero-length!");
        return default_value;
    }

    /* get library attributes */
    if(id == 0) {
        if(str_cmp_i(attrib_name, "version_major") == 0) {
            res = (int)version_major;
        } else if(str_cmp_i(attrib_name, "version_minor") == 0) {
            res = (int)version_minor;
        } else if(str_cmp_i(attrib_name, "version_patch") == 0) {
            res = (int)version_patch;
        } else if(str_cmp_i(attrib_name, "debug") == 0) {
            res = (int)get_debug_level();
        } else if(str_cmp_i(attrib_name, "debug_level") == 0) {
            pdebug(DEBUG_WARN, "Deprecated attribute \"debug_level\" used, use \"debug\" instead.");
            res = (int)get_debug_level();
        } else {
            pdebug(DEBUG_WARN, "Attribute \"%s\" is not supported at the library level!");
            res = default_value;
        }
    } else {
        tag = lookup_tag(id);

        if(!tag) {
            pdebug(DEBUG_WARN,"Tag not found.");
            return default_value;
        }

        critical_block(tag->api_mutex) {
            /* match the generic ones first. */
            if(str_cmp_i(attrib_name, "size") == 0) {
                tag->status = PLCTAG_STATUS_OK;
                res = (int)tag->size;
            } else if(str_cmp_i(attrib_name, "read_cache_ms") == 0) {
                /* FIXME - what happens if this overflows? */
                tag->status = PLCTAG_STATUS_OK;
                res = (int)tag->read_cache_ms;
            } else if(str_cmp_i(attrib_name, "auto_sync_read_ms") == 0) {
                tag->status = PLCTAG_STATUS_OK;
                res = (int)tag->auto_sync_read_ms;
            } else if(str_cmp_i(attrib_name, "auto_sync_write_ms") == 0) {
                tag->status = PLCTAG_STATUS_OK;
                res = (int)tag->auto_sync_write_ms;
            } else if(str_cmp_i(attrib_name, "bit_num") == 0) {
                tag->status = PLCTAG_STATUS_OK;
                res = (int)(unsigned int)(tag->bit);
            } else if(str_cmp_i(attrib_name, "connection_group_id") == 0) {
                pdebug(DEBUG_DETAIL, "Getting the connection_group_id for tag %" PRId32 ".", id);
                tag->status = PLCTAG_STATUS_OK;
                res = tag->connection_group_id;
            } else {
                if(tag->vtable->get_int_attrib) {
                    res = tag->vtable->get_int_attrib(tag, attrib_name, default_value);
                }
            }
        }

        rc_dec(tag);
    }

    pdebug(DEBUG_SPEW, "Done.");

    return res;
}



LIB_EXPORT int plc_tag_set_int_attribute(int32_t id, const char *attrib_name, int new_value)
{
    int res = PLCTAG_ERR_NOT_FOUND;
    plc_tag_p tag = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!attrib_name || str_length(attrib_name) == 0) {
        pdebug(DEBUG_WARN, "Attribute name must not be null or zero-length!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* get library attributes */
    if(id == 0) {
        if(str_cmp_i(attrib_name, "debug") == 0) {
            if(new_value >= DEBUG_ERROR && new_value < DEBUG_SPEW) {
                set_debug_level(new_value);
                res = PLCTAG_STATUS_OK;
            } else {
                res = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        } else if(str_cmp_i(attrib_name, "debug_level") == 0) {
            pdebug(DEBUG_WARN, "Deprecated attribute \"debug_level\" used, use \"debug\" instead.");
            if(new_value >= DEBUG_ERROR && new_value < DEBUG_SPEW) {
                set_debug_level(new_value);
                res = PLCTAG_STATUS_OK;
            } else {
                res = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        } else {
            pdebug(DEBUG_WARN, "Attribute \"%s\" is not support at the library level!", attrib_name);
            return PLCTAG_ERR_UNSUPPORTED;
        }
    } else {
        tag = lookup_tag(id);

        if(!tag) {
            pdebug(DEBUG_WARN,"Tag not found.");
            return PLCTAG_ERR_NOT_FOUND;
        }

        critical_block(tag->api_mutex) {
            /* match the generic ones first. */
            if(str_cmp_i(attrib_name, "read_cache_ms") == 0) {
                if(new_value >= 0) {
                    /* expire the cache. */
                    tag->read_cache_expire = (int64_t)0;
                    tag->read_cache_ms = (int64_t)new_value;
                    tag->status = PLCTAG_STATUS_OK;
                    res = PLCTAG_STATUS_OK;
                } else {
                    tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                    res = PLCTAG_ERR_OUT_OF_BOUNDS;
                }
            } else if(str_cmp_i(attrib_name, "auto_sync_read_ms") == 0) {
                if(new_value >= 0) {
                    tag->auto_sync_read_ms = new_value;
                    tag->status = PLCTAG_STATUS_OK;
                    res = PLCTAG_STATUS_OK;
                } else {
                    pdebug(DEBUG_WARN, "auto_sync_read_ms must be greater than or equal to zero!");
                    tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                    res = PLCTAG_ERR_OUT_OF_BOUNDS;
                }
            } else if(str_cmp_i(attrib_name, "auto_sync_write_ms") == 0) {
                if(new_value >= 0) {
                    tag->auto_sync_write_ms = new_value;
                    tag->status = PLCTAG_STATUS_OK;
                    res = PLCTAG_STATUS_OK;
                } else {
                    pdebug(DEBUG_WARN, "auto_sync_write_ms must be greater than or equal to zero!");
                    tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                    res = PLCTAG_ERR_OUT_OF_BOUNDS;
                }
            } else {
                if(tag->vtable->set_int_attrib) {
                    res = tag->vtable->set_int_attrib(tag, attrib_name, new_value);
                    tag->status = (int8_t)res;
                }
            }
        }
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done.");

    return res;
}




LIB_EXPORT int plc_tag_get_size(int32_t id)
{
    int result = 0;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    critical_block(tag->api_mutex) {
        result = tag->size;
        tag->status = PLCTAG_STATUS_OK;
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done.");

    return result;
}



LIB_EXPORT int plc_tag_set_size(int32_t id, int new_size)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_DETAIL, "Starting with new size %d.", new_size);

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    if(new_size < 0) {
        pdebug(DEBUG_WARN, "Illegal new size %d bytes for tag is illegal.  Tag size must be positive.");
        rc_dec(tag);
        return PLCTAG_ERR_BAD_PARAM;
    }

    critical_block(tag->api_mutex) {
        uint8_t *new_data = (uint8_t*)mem_realloc(tag->data, new_size);

        if(new_data) {
            /* return the old size */
            rc = tag->size;
            tag->data = new_data;
            tag->size = new_size;
            tag->status = PLCTAG_STATUS_OK;
        } else {
            rc = (int)PLCTAG_ERR_NO_MEM;
            tag->status = (int8_t)rc;
        }
    }

    rc_dec(tag);

    if(rc >= 0) {
        pdebug(DEBUG_DETAIL, "Done with old size %d.", rc);
    } else {
        pdebug(DEBUG_WARN, "Tag buffer resize failed with error %s!", plc_tag_decode_error(rc));
    }

    return rc;
}





LIB_EXPORT int plc_tag_get_bit(int32_t id, int offset_bit)
{
    int res = PLCTAG_ERR_OUT_OF_BOUNDS;
    int real_offset = offset_bit;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN, "Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    /* if this is a single bit, then make sure the offset is the tag bit. */
    if(tag->is_bit) {
        real_offset = tag->bit;
    } else {
        real_offset = offset_bit;
    }

    pdebug(DEBUG_SPEW, "selecting bit %d with offset %d in byte %d (%x).", real_offset, (real_offset % 8), (real_offset / 8), tag->data[real_offset / 8]);

    critical_block(tag->api_mutex) {
        if((real_offset >= 0) && ((real_offset / 8) < tag->size)) {
            res = !!(((1 << (real_offset % 8)) & 0xFF) & (tag->data[real_offset / 8]));
            tag->status = PLCTAG_STATUS_OK;
        } else {
            pdebug(DEBUG_WARN, "Data offset out of bounds!");
            res = PLCTAG_ERR_OUT_OF_BOUNDS;
            tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
        }
    }

    rc_dec(tag);

    return res;
}


LIB_EXPORT int plc_tag_set_bit(int32_t id, int offset_bit, int val)
{
    int res = PLCTAG_STATUS_OK;
    int real_offset = offset_bit;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN, "Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    /* if this is a single bit, then make sure the offset is the tag bit. */
    if(tag->is_bit) {
        real_offset = tag->bit;
    } else {
        real_offset = offset_bit;
    }

    pdebug(DEBUG_SPEW, "Setting bit %d with offset %d in byte %d (%x).", real_offset, (real_offset % 8), (real_offset / 8), tag->data[real_offset / 8]);

    critical_block(tag->api_mutex) {
        if((real_offset >= 0) && ((real_offset / 8) < tag->size)) {
            if(tag->auto_sync_write_ms > 0) {
                tag->tag_is_dirty = 1;
            }

            if(val) {
                tag->data[real_offset / 8] |= (uint8_t)(1 << (real_offset % 8));
            } else {
                tag->data[real_offset / 8] &= (uint8_t)(~(1 << (real_offset % 8)));
            }

            tag->status = PLCTAG_STATUS_OK;
        } else {
            pdebug(DEBUG_WARN, "Data offset out of bounds!");
            tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            res = PLCTAG_ERR_OUT_OF_BOUNDS;
        }
    }

    rc_dec(tag);

    return res;
}



LIB_EXPORT uint64_t plc_tag_get_uint64(int32_t id, int offset)
{
    uint64_t res = UINT64_MAX;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint64_t)) <= tag->size)) {
                res =   ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[0]]) << 0 ) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[1]]) << 8 ) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[2]]) << 16) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[3]]) << 24) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[4]]) << 32) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[5]]) << 40) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[6]]) << 48) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[7]]) << 56);

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (unsigned int)rc;
        }
    }

    rc_dec(tag);

    return res;
}



LIB_EXPORT int plc_tag_set_uint64(int32_t id, int offset, uint64_t val)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint64_t)) <= tag->size)) {
                if(tag->auto_sync_write_ms > 0) {
                    tag->tag_is_dirty = 1;
                }

                tag->data[offset + tag->byte_order->int64_order[0]] = (uint8_t)((val >> 0 ) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[1]] = (uint8_t)((val >> 8 ) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[2]] = (uint8_t)((val >> 16) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[3]] = (uint8_t)((val >> 24) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[4]] = (uint8_t)((val >> 32) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[5]] = (uint8_t)((val >> 40) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[6]] = (uint8_t)((val >> 48) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[7]] = (uint8_t)((val >> 56) & 0xFF);

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        if(!val) {
            rc = plc_tag_set_bit(id, 0, 0);
        } else {
            rc = plc_tag_set_bit(id, 0, 1);
        }
    }

    rc_dec(tag);

    return rc;
}




LIB_EXPORT int64_t plc_tag_get_int64(int32_t id, int offset)
{
    int64_t res = INT64_MIN;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(int64_t)) <= tag->size)) {
                res = (int64_t)(((uint64_t)(tag->data[offset + tag->byte_order->int64_order[0]]) << 0 ) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[1]]) << 8 ) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[2]]) << 16) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[3]]) << 24) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[4]]) << 32) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[5]]) << 40) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[6]]) << 48) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[7]]) << 56));

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = rc;
        }
    }

    rc_dec(tag);

    return res;
}



LIB_EXPORT int plc_tag_set_int64(int32_t id, int offset, int64_t ival)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);
    uint64_t val = (uint64_t)(ival);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(int64_t)) <= tag->size)) {
                if(tag->auto_sync_write_ms > 0) {
                    tag->tag_is_dirty = 1;
                }

                tag->data[offset + tag->byte_order->int64_order[0]] = (uint8_t)((val >> 0 ) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[1]] = (uint8_t)((val >> 8 ) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[2]] = (uint8_t)((val >> 16) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[3]] = (uint8_t)((val >> 24) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[4]] = (uint8_t)((val >> 32) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[5]] = (uint8_t)((val >> 40) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[6]] = (uint8_t)((val >> 48) & 0xFF);
                tag->data[offset + tag->byte_order->int64_order[7]] = (uint8_t)((val >> 56) & 0xFF);

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        if(!val) {
            rc = plc_tag_set_bit(id, 0, 0);
        } else {
            rc = plc_tag_set_bit(id, 0, 1);
        }
    }

    rc_dec(tag);

    return rc;
}

LIB_EXPORT uint32_t plc_tag_get_uint32(int32_t id, int offset)
{
    uint32_t res = UINT32_MAX;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint32_t)) <= tag->size)) {
                res =   ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[0]]) << 0 ) +
                        ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[1]]) << 8 ) +
                        ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[2]]) << 16) +
                        ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[3]]) << 24);

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (unsigned int)rc;
        }
    }

    rc_dec(tag);

    return res;
}



LIB_EXPORT int plc_tag_set_uint32(int32_t id, int offset, uint32_t val)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint32_t)) <= tag->size)) {
                if(tag->auto_sync_write_ms > 0) {
                    tag->tag_is_dirty = 1;
                }

                tag->data[offset + tag->byte_order->int32_order[0]] = (uint8_t)((val >> 0 ) & 0xFF);
                tag->data[offset + tag->byte_order->int32_order[1]] = (uint8_t)((val >> 8 ) & 0xFF);
                tag->data[offset + tag->byte_order->int32_order[2]] = (uint8_t)((val >> 16) & 0xFF);
                tag->data[offset + tag->byte_order->int32_order[3]] = (uint8_t)((val >> 24) & 0xFF);

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        if(!val) {
            rc = plc_tag_set_bit(id, 0, 0);
        } else {
            rc = plc_tag_set_bit(id, 0, 1);
        }
    }

    rc_dec(tag);

    return rc;
}




LIB_EXPORT int32_t  plc_tag_get_int32(int32_t id, int offset)
{
    int32_t res = INT32_MIN;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(int32_t)) <= tag->size)) {
                res = (int32_t)(((uint32_t)(tag->data[offset + tag->byte_order->int32_order[0]]) << 0 ) +
                                ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[1]]) << 8 ) +
                                ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[2]]) << 16) +
                                ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[3]]) << 24));

                tag->status = PLCTAG_STATUS_OK;
            }  else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (int32_t)rc;
        }
    }

    rc_dec(tag);

    return res;
}



LIB_EXPORT int plc_tag_set_int32(int32_t id, int offset, int32_t ival)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);
    uint32_t val = (uint32_t)ival;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(int32_t)) <= tag->size)) {
                if(tag->auto_sync_write_ms > 0) {
                    tag->tag_is_dirty = 1;
                }

                tag->data[offset + tag->byte_order->int32_order[0]] = (uint8_t)((val >> 0 ) & 0xFF);
                tag->data[offset + tag->byte_order->int32_order[1]] = (uint8_t)((val >> 8 ) & 0xFF);
                tag->data[offset + tag->byte_order->int32_order[2]] = (uint8_t)((val >> 16) & 0xFF);
                tag->data[offset + tag->byte_order->int32_order[3]] = (uint8_t)((val >> 24) & 0xFF);

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        if(!val) {
            rc = plc_tag_set_bit(id, 0, 0);
        } else {
            rc = plc_tag_set_bit(id, 0, 1);
        }
    }

    rc_dec(tag);

    return rc;
}







LIB_EXPORT uint16_t plc_tag_get_uint16(int32_t id, int offset)
{
    uint16_t res = UINT16_MAX;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint16_t)) <= tag->size)) {
                res =   (uint16_t)(((uint16_t)(tag->data[offset + tag->byte_order->int16_order[0]]) << 0 ) +
                                   ((uint16_t)(tag->data[offset + tag->byte_order->int16_order[1]]) << 8 ));

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (uint16_t)(unsigned int)rc;
        }
    }

    rc_dec(tag);

    return res;
}




LIB_EXPORT int plc_tag_set_uint16(int32_t id, int offset, uint16_t val)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint16_t)) <= tag->size)) {
                if(tag->auto_sync_write_ms > 0) {
                    tag->tag_is_dirty = 1;
                }

                tag->data[offset + tag->byte_order->int16_order[0]] = (uint8_t)((val >> 0 ) & 0xFF);
                tag->data[offset + tag->byte_order->int16_order[1]] = (uint8_t)((val >> 8 ) & 0xFF);

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        if(!val) {
            rc = plc_tag_set_bit(id, 0, 0);
        } else {
            rc = plc_tag_set_bit(id, 0, 1);
        }
    }

    rc_dec(tag);

    return rc;
}








LIB_EXPORT int16_t  plc_tag_get_int16(int32_t id, int offset)
{
    int16_t res = INT16_MIN;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(int16_t)) <= tag->size)) {
                res =   (int16_t)(uint16_t)(((uint16_t)(tag->data[offset + tag->byte_order->int16_order[0]]) << 0 ) +
                                            ((uint16_t)(tag->data[offset + tag->byte_order->int16_order[1]]) << 8 ));
                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (int16_t)(uint16_t)(unsigned int)rc;
        }
    }

    rc_dec(tag);

    return res;
}




LIB_EXPORT int plc_tag_set_int16(int32_t id, int offset, int16_t ival)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);
    uint16_t val = (uint16_t)ival;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(int16_t)) <= tag->size)) {
                if(tag->auto_sync_write_ms > 0) {
                    tag->tag_is_dirty = 1;
                }

                tag->data[offset + tag->byte_order->int16_order[0]] = (uint8_t)((val >> 0 ) & 0xFF);
                tag->data[offset + tag->byte_order->int16_order[1]] = (uint8_t)((val >> 8 ) & 0xFF);

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        if(!val) {
            rc = plc_tag_set_bit(id, 0, 0);
        } else {
            rc = plc_tag_set_bit(id, 0, 1);
        }
    }

    rc_dec(tag);

    return rc;
}








LIB_EXPORT uint8_t plc_tag_get_uint8(int32_t id, int offset)
{
    uint8_t res = UINT8_MAX;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint8_t)) <= tag->size)) {
                res = tag->data[offset];
                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (uint8_t)(unsigned int)rc;
        }
    }

    rc_dec(tag);

    return res;
}




LIB_EXPORT int plc_tag_set_uint8(int32_t id, int offset, uint8_t val)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint8_t)) <= tag->size)) {
                if(tag->auto_sync_write_ms > 0) {
                    tag->tag_is_dirty = 1;
                }

                tag->data[offset] = val;

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        if(!val) {
            rc = plc_tag_set_bit(id, 0, 0);
        } else {
            rc = plc_tag_set_bit(id, 0, 1);
        }
    }

    rc_dec(tag);

    return rc;
}





LIB_EXPORT int8_t plc_tag_get_int8(int32_t id, int offset)
{
    int8_t res = INT8_MIN;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint8_t)) <= tag->size)) {
                res =   (int8_t)tag->data[offset];
                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (int8_t)(unsigned int)rc;
        }
    }

    rc_dec(tag);

    return res;
}




LIB_EXPORT int plc_tag_set_int8(int32_t id, int offset, int8_t ival)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);
    uint8_t val = (uint8_t)ival;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(int8_t)) <= tag->size)) {
                if(tag->auto_sync_write_ms > 0) {
                    tag->tag_is_dirty = 1;
                }

                tag->data[offset] = val;

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        if(!val) {
            rc = plc_tag_set_bit(id, 0, 0);
        } else {
            rc = plc_tag_set_bit(id, 0, 1);
        }
    }

    rc_dec(tag);

    return rc;
}






LIB_EXPORT double plc_tag_get_float64(int32_t id, int offset)
{
    double res = DBL_MIN;
    int rc = PLCTAG_STATUS_OK;
    uint64_t ures = 0;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(tag->is_bit) {
        pdebug(DEBUG_WARN, "Getting float64 value is unsupported on a bit tag!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc_dec(tag);
        return res;
    }

    critical_block(tag->api_mutex) {
        if((offset >= 0) && (offset + ((int)sizeof(double)) <= tag->size)) {
            ures =  ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[0]]) << 0 ) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[1]]) << 8 ) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[2]]) << 16) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[3]]) << 24) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[4]]) << 32) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[5]]) << 40) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[6]]) << 48) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[7]]) << 56);

            tag->status = PLCTAG_STATUS_OK;
            rc = PLCTAG_STATUS_OK;
        } else {
            pdebug(DEBUG_WARN, "Data offset out of bounds!");
            tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            rc = PLCTAG_ERR_OUT_OF_BOUNDS;
        }
    }

    if(rc == PLCTAG_STATUS_OK) {
        /* copy the data */
        mem_copy(&res,&ures,sizeof(res));
    } else {
        res = DBL_MIN;
    }

    rc_dec(tag);

    return res;
}




LIB_EXPORT int plc_tag_set_float64(int32_t id, int offset, double fval)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);
    uint64_t val = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    if(tag->is_bit) {
        pdebug(DEBUG_WARN, "Setting float64 value is unsupported on a bit tag!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc_dec(tag);
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* copy the data into the uint64 value */
    mem_copy(&val, &fval, sizeof(val));

    critical_block(tag->api_mutex) {
        if((offset >= 0) && (offset + ((int)sizeof(uint64_t)) <= tag->size)) {
            if(tag->auto_sync_write_ms > 0) {
                tag->tag_is_dirty = 1;
            }

            tag->data[offset + tag->byte_order->float64_order[0]] = (uint8_t)((val >> 0 ) & 0xFF);
            tag->data[offset + tag->byte_order->float64_order[1]] = (uint8_t)((val >> 8 ) & 0xFF);
            tag->data[offset + tag->byte_order->float64_order[2]] = (uint8_t)((val >> 16) & 0xFF);
            tag->data[offset + tag->byte_order->float64_order[3]] = (uint8_t)((val >> 24) & 0xFF);
            tag->data[offset + tag->byte_order->float64_order[4]] = (uint8_t)((val >> 32) & 0xFF);
            tag->data[offset + tag->byte_order->float64_order[5]] = (uint8_t)((val >> 40) & 0xFF);
            tag->data[offset + tag->byte_order->float64_order[6]] = (uint8_t)((val >> 48) & 0xFF);
            tag->data[offset + tag->byte_order->float64_order[7]] = (uint8_t)((val >> 56) & 0xFF);

            tag->status = PLCTAG_STATUS_OK;
        } else {
            pdebug(DEBUG_WARN, "Data offset out of bounds!");
            tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            rc = PLCTAG_ERR_OUT_OF_BOUNDS;
        }
    }

    rc_dec(tag);

    return rc;
}



LIB_EXPORT float plc_tag_get_float32(int32_t id, int offset)
{
    float res = FLT_MIN;
    int rc = PLCTAG_STATUS_OK;
    uint32_t ures = 0;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(tag->is_bit) {
        pdebug(DEBUG_WARN, "Getting float32 value is unsupported on a bit tag!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc_dec(tag);
        return res;
    }

    critical_block(tag->api_mutex) {
        if((offset >= 0) && (offset + ((int)sizeof(float)) <= tag->size)) {
            ures =  (uint32_t)(((uint32_t)(tag->data[offset + tag->byte_order->float32_order[0]]) << 0 ) +
                               ((uint32_t)(tag->data[offset + tag->byte_order->float32_order[1]]) << 8 ) +
                               ((uint32_t)(tag->data[offset + tag->byte_order->float32_order[2]]) << 16) +
                               ((uint32_t)(tag->data[offset + tag->byte_order->float32_order[3]]) << 24));

            tag->status = PLCTAG_STATUS_OK;
            rc = PLCTAG_STATUS_OK;
        } else {
            pdebug(DEBUG_WARN, "Data offset out of bounds!");
            tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            rc = PLCTAG_ERR_OUT_OF_BOUNDS;
        }
    }

    if(rc == PLCTAG_STATUS_OK) {
        /* copy the data */
        mem_copy(&res,&ures,sizeof(res));
    } else {
        res = FLT_MIN;
    }

    rc_dec(tag);

    return res;
}




LIB_EXPORT int plc_tag_set_float32(int32_t id, int offset, float fval)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);
    uint32_t val = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    if(tag->is_bit) {
        pdebug(DEBUG_WARN, "Setting float32 value is unsupported on a bit tag!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc_dec(tag);
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* copy the data into the uint64 value */
    mem_copy(&val, &fval, sizeof(val));

    critical_block(tag->api_mutex) {
        if((offset >= 0) && (offset + ((int)sizeof(float)) <= tag->size)) {
            if(tag->auto_sync_write_ms > 0) {
                tag->tag_is_dirty = 1;
            }

            tag->data[offset + tag->byte_order->float32_order[0]] = (uint8_t)((val >> 0 ) & 0xFF);
            tag->data[offset + tag->byte_order->float32_order[1]] = (uint8_t)((val >> 8 ) & 0xFF);
            tag->data[offset + tag->byte_order->float32_order[2]] = (uint8_t)((val >> 16) & 0xFF);
            tag->data[offset + tag->byte_order->float32_order[3]] = (uint8_t)((val >> 24) & 0xFF);

            tag->status = PLCTAG_STATUS_OK;
        } else {
            pdebug(DEBUG_WARN, "Data offset out of bounds!");
            tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            rc = PLCTAG_ERR_OUT_OF_BOUNDS;
        }
    }

    rc_dec(tag);

    return rc;
}


LIB_EXPORT int plc_tag_get_string(int32_t tag_id, int string_start_offset, char *buffer, int buffer_length)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(tag_id);
    int max_len = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* are strings defined for this tag? */
    if(!tag->byte_order || !tag->byte_order->str_is_defined) {
        pdebug(DEBUG_WARN,"Tag has no definitions for strings!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc_dec(tag);
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    if(tag->is_bit) {
        pdebug(DEBUG_WARN, "Getting a string value from a bit tag is not supported!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc_dec(tag);
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* set all buffer bytes to zero. */
    mem_set(buffer, 0, buffer_length);

    critical_block(tag->api_mutex) {
        int string_length = get_string_length_unsafe(tag, string_start_offset);

        /* determine the maximum number of characters/bytes to copy. */
        if(buffer_length < string_length) {
            pdebug(DEBUG_WARN, "Buffer length, %d, is less than the string length, %d!", buffer_length, string_length);
            max_len = buffer_length;
        } else {
            max_len = string_length;
        }

        /* check the amount of space. */
        if(string_start_offset + (int)tag->byte_order->str_count_word_bytes + max_len <= tag->size) {
            for(int i = 0; i < max_len; i++) {
                size_t char_index = (((size_t)(unsigned int)i) ^ (tag->byte_order->str_is_byte_swapped)) /* byte swap if necessary */
                                  + (size_t)(unsigned int)string_start_offset
                                  + (size_t)(unsigned int)(tag->byte_order->str_count_word_bytes);
                buffer[i] = (char)tag->data[char_index];
            }

            tag->status = PLCTAG_STATUS_OK;
            rc = PLCTAG_STATUS_OK;
        } else {
            pdebug(DEBUG_WARN, "Data offset out of bounds!");
            tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            rc = PLCTAG_ERR_OUT_OF_BOUNDS;
        }
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}


LIB_EXPORT int plc_tag_set_string(int32_t tag_id, int string_start_offset, const char *string_val)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(tag_id);
    int string_length = 0;

    pdebug(DEBUG_DETAIL, "Starting with string %s.", string_val);

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    /* are strings defined for this tag? */
    if(!tag->byte_order || !tag->byte_order->str_is_defined) {
        pdebug(DEBUG_WARN,"Tag has no definitions for strings!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc_dec(tag);
        return PLCTAG_ERR_UNSUPPORTED;
    }

    if(!string_val) {
        pdebug(DEBUG_WARN, "New string value pointer is null!");
        tag->status = PLCTAG_ERR_NULL_PTR;
        rc_dec(tag);
        return PLCTAG_ERR_NULL_PTR;
    }

    /* note that passing a zero-length string is valid. */

    if(tag->is_bit) {
        pdebug(DEBUG_WARN, "Setting a string value on a bit tag is not supported!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc_dec(tag);
        return PLCTAG_ERR_UNSUPPORTED;
    }

    string_length = str_length(string_val);

    critical_block(tag->api_mutex) {
        int string_capacity = (tag->byte_order->str_max_capacity ? (int)(tag->byte_order->str_max_capacity) : get_string_length_unsafe(tag, string_start_offset));
        int string_last_offset = string_start_offset + (int)(tag->byte_order->str_count_word_bytes) + string_capacity + (tag->byte_order->str_is_zero_terminated ? 1 : 0);

        /* initial checks - these must be done in the critical block otherwise someone could change the tag buffer size etc. underneath us. */
pdebug(DEBUG_WARN, "string_capacity=%d, string_last_offset=%d, tag_size=%d.", string_capacity, string_last_offset, (int)tag->size);

        /* will it fit at all? */
        if(string_capacity < string_length) {
            pdebug(DEBUG_WARN, "Passed string value is longer than allowed string capacity!");
            rc = PLCTAG_ERR_TOO_LARGE;
            tag->status = (int8_t)rc;
            break;
        }

        /* do we have a mismatch between what is in the tag buffer and the string or configuration? */
        if(string_last_offset > tag->size) {
            pdebug(DEBUG_WARN, "Bad configuration? String capacity/size is larger than the tag buffer!");
            rc = PLCTAG_ERR_BAD_CONFIG;
            tag->status = (int8_t)rc;
            break;
        }

        /* copy the string data into the tag. */
        rc = PLCTAG_STATUS_OK;
        for(int i = 0; i < string_length; i++) {
            size_t char_index = (((size_t)(unsigned int)i) ^ (tag->byte_order->str_is_byte_swapped)) /* byte swap if necessary */
                            + (size_t)(unsigned int)string_start_offset
                            + (size_t)(unsigned int)(tag->byte_order->str_count_word_bytes);

            if(char_index < (size_t)(uint32_t)tag->size) {
                tag->data[char_index] = (uint8_t)string_val[i];
            } else {
                pdebug(DEBUG_WARN, "Out of bounds index generated during string copy!");
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;

                /* note: only breaks out of the for loop, we need another break. */
                break;
            }
        }

        /* break out of the critical block if bad status. */
        if(rc != PLCTAG_STATUS_OK) {
            tag->status = (int8_t)rc;
            break;
        }

        /* if the string is counted, set the length */
        if(tag->byte_order->str_is_counted) {
            int last_count_word_index = string_start_offset + (int)(unsigned int)tag->byte_order->str_count_word_bytes;

            if(last_count_word_index > (int)(tag->size)) {
                pdebug(DEBUG_WARN, "Unable to write valid count word as count word would go past the end of the tag buffer!");
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;
                tag->status = (int8_t)rc;
                break;
            }

            switch(tag->byte_order->str_count_word_bytes) {
                case 1:
                    tag->data[string_start_offset] = (uint8_t)(unsigned int)string_length;
                    break;

                case 2:
                    tag->data[string_start_offset + tag->byte_order->int16_order[0]] = (uint8_t)((((unsigned int)string_length) >> 0 ) & 0xFF);
                    tag->data[string_start_offset + tag->byte_order->int16_order[1]] = (uint8_t)((((unsigned int)string_length) >> 8 ) & 0xFF);
                    break;

                case 4:
                    tag->data[string_start_offset + tag->byte_order->int32_order[0]] = (uint8_t)((((unsigned int)string_length) >> 0 ) & 0xFF);
                    tag->data[string_start_offset + tag->byte_order->int32_order[1]] = (uint8_t)((((unsigned int)string_length) >> 8 ) & 0xFF);
                    tag->data[string_start_offset + tag->byte_order->int32_order[2]] = (uint8_t)((((unsigned int)string_length) >> 16) & 0xFF);
                    tag->data[string_start_offset + tag->byte_order->int32_order[3]] = (uint8_t)((((unsigned int)string_length) >> 24) & 0xFF);
                    break;

                default:
                    pdebug(DEBUG_WARN, "Unsupported string count size, %d!", tag->byte_order->str_count_word_bytes);
                    rc = PLCTAG_ERR_UNSUPPORTED;
                    tag->status = (int8_t)rc;
                    break;
            }
        }

        /* zero pad the rest. */
        rc = PLCTAG_STATUS_OK;
        for(int i = string_length; i < string_capacity; i++) {
            size_t char_index = (((size_t)(unsigned int)i) ^ (tag->byte_order->str_is_byte_swapped)) /* byte swap if necessary */
                            + (size_t)(unsigned int)string_start_offset
                            + (size_t)(unsigned int)(tag->byte_order->str_count_word_bytes);

            if(char_index < (size_t)(uint32_t)tag->size) {
                tag->data[char_index] = (uint8_t)0;
            } else {
                pdebug(DEBUG_WARN, "Out of bounds index generated during string zero padding!");
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;

                /* note: only breaks out of the for loop, we need another break. */
                break;
            }
        }

        /* break out of the critical block if bad status. */
        if(rc != PLCTAG_STATUS_OK) {
            tag->status = (int8_t)rc;
            break;
        }

        /* if this is an auto-write tag, set the dirty flag to eventually trigger a write */
        if(rc == PLCTAG_STATUS_OK && tag->auto_sync_write_ms > 0) {
            tag->tag_is_dirty = 1;
        }

        /* set the return and tag status. */
        rc = PLCTAG_STATUS_OK;
        tag->status = (int8_t)rc;
    }

    rc_dec(tag);

    pdebug(DEBUG_DETAIL, "Done with status %s.", plc_tag_decode_error(rc));

    return rc;
}




LIB_EXPORT int plc_tag_get_string_capacity(int32_t id, int string_start_offset)
{
    int string_capacity = 0;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* are strings defined for this tag? */
    if(!tag->byte_order || !tag->byte_order->str_is_defined) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no definitions for strings!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* is there data? */
    if(!tag->data) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        return PLCTAG_ERR_NO_DATA;
    }

    if(tag->is_bit) {
        rc_dec(tag);
        pdebug(DEBUG_WARN, "Getting string capacity from a bit tag is not supported!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        return PLCTAG_ERR_UNSUPPORTED;
    }

    critical_block(tag->api_mutex) {
        string_capacity = (tag->byte_order->str_max_capacity ? (int)(tag->byte_order->str_max_capacity) : get_string_length_unsafe(tag, string_start_offset));
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done.");

    return string_capacity;
}



LIB_EXPORT int plc_tag_get_string_length(int32_t id, int string_start_offset)
{
    int string_length = 0;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* are strings defined for this tag? */
    if(!tag->byte_order || !tag->byte_order->str_is_defined) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no definitions for strings!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* is there data? */
    if(!tag->data) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        return PLCTAG_ERR_NO_DATA;
    }

    if(tag->is_bit) {
        rc_dec(tag);
        pdebug(DEBUG_WARN, "Getting string length from a bit tag is not supported!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        return PLCTAG_ERR_UNSUPPORTED;
    }

    critical_block(tag->api_mutex) {
        string_length = get_string_length_unsafe(tag, string_start_offset);
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done.");

    return string_length;
}




LIB_EXPORT int plc_tag_get_string_total_length(int32_t id, int string_start_offset)
{
    int total_length = 0;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* are strings defined for this tag? */
    if(!tag->byte_order || !tag->byte_order->str_is_defined) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no definitions for strings!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* is there data? */
    if(!tag->data) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        return PLCTAG_ERR_NO_DATA;
    }

    if(tag->is_bit) {
        rc_dec(tag);
        pdebug(DEBUG_WARN, "Getting a string total length from a bit tag is not supported!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        return PLCTAG_ERR_UNSUPPORTED;
    }

    critical_block(tag->api_mutex) {
        total_length = (int)(tag->byte_order->str_count_word_bytes)
                     + (tag->byte_order->str_is_fixed_length ? (int)(tag->byte_order->str_max_capacity) : get_string_length_unsafe(tag, string_start_offset))
                     + (tag->byte_order->str_is_zero_terminated ? (int)1 : (int)0)
                     + (int)(tag->byte_order->str_pad_bytes);
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done.");

    return total_length;
}



LIB_EXPORT int plc_tag_set_raw_bytes(int32_t id, int offset, uint8_t *buffer, int buffer_size)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        return PLCTAG_ERR_NO_DATA;
    }

    if(!buffer) {
        pdebug(DEBUG_WARN,"Buffer is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(buffer_size <= 0) {
        pdebug(DEBUG_WARN,"The buffer must have some capacity for data.");
        return PLCTAG_ERR_BAD_PARAM;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && ((offset + buffer_size) <= tag->size)) {
                if(tag->auto_sync_write_ms > 0) {
                    tag->tag_is_dirty = 1;
                }

                int i;
                for (i=0;i<buffer_size;i++) {
                    tag->data[offset + i] = buffer[i];
                }

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        pdebug(DEBUG_WARN,"Trying to write a list of value on a Tag bit.");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc=PLCTAG_ERR_UNSUPPORTED;
    }

    rc_dec(tag);

    return rc;
}


LIB_EXPORT int plc_tag_get_raw_bytes(int32_t id, int offset, uint8_t *buffer, int buffer_size)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        return PLCTAG_ERR_NO_DATA;
    }

    if(!buffer) {
        pdebug(DEBUG_WARN,"Buffer is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(buffer_size <= 0) {
        pdebug(DEBUG_WARN,"The buffer must have some capacity for data.");
        return PLCTAG_ERR_BAD_PARAM;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && ((offset + buffer_size) <= tag->size)) {
                int i;
                for (i=0;i<buffer_size;i++) {
                    buffer[i] = tag->data[offset + i];
                }

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        pdebug(DEBUG_WARN,"Trying to read a list of values from a Tag bit.");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc = PLCTAG_ERR_UNSUPPORTED;
    }

    rc_dec(tag);

    return rc;
}




/*****************************************************************************************************
 *****************************  Support routines for extra indirection *******************************
 ****************************************************************************************************/

int set_tag_byte_order(plc_tag_p tag, attr attribs)

{
    int use_default = 1;

    pdebug(DEBUG_INFO, "Starting.");

    /* the default values are already set in the tag. */

    /* check for overrides. */
    if(attr_get_str(attribs,  "int16_byte_order", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs,  "int32_byte_order", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs,  "int64_byte_order", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs,  "float32_byte_order", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs,  "float64_byte_order", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_is_counted", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_is_fixed_length", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_is_zero_terminated", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_is_byte_swapped", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_count_word_bytes", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_max_capacity", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_total_length", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_pad_bytes", NULL) != NULL) {
        use_default = 0;
    }

    /* if we need to override something, build a new byte order structure. */
    if(!use_default) {
        const char *byte_order_str = NULL;
        int str_param = 0;
        int rc = PLCTAG_STATUS_OK;
        tag_byte_order_t *new_byte_order = (tag_byte_order_t*)mem_alloc((int)(unsigned int)sizeof(*(tag->byte_order)));

        if(!new_byte_order) {
            pdebug(DEBUG_WARN, "Unable to allocate byte order struct for tag!");
            return PLCTAG_ERR_NO_MEM;
        }

        /* copy the defaults. */
        *new_byte_order = *(tag->byte_order);

        /* replace the old byte order. */
        tag->byte_order = new_byte_order;

        /* mark it as allocated so that we free it later. */
        tag->byte_order->is_allocated = 1;

        /* 16-bit ints. */
        byte_order_str = attr_get_str(attribs, "int16_byte_order", NULL);
        if(byte_order_str) {
            pdebug(DEBUG_DETAIL, "Override byte order int16_byte_order=%s", byte_order_str);

            rc = check_byte_order_str(byte_order_str, 2);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Byte order string int16_byte_order, \"%s\", is illegal or malformed.", byte_order_str);
                return rc;
            }

            /* strange gyrations to make the compiler happy.   MSVC will probably complain. */
            tag->byte_order->int16_order[0] = (int)(unsigned int)(((unsigned int)byte_order_str[0] - (unsigned int)('0')) & 0x01);
            tag->byte_order->int16_order[1] = (int)(unsigned int)(((unsigned int)byte_order_str[1] - (unsigned int)('0')) & 0x01);
        }

        /* 32-bit ints. */
        byte_order_str = attr_get_str(attribs, "int32_byte_order", NULL);
        if(byte_order_str) {
            pdebug(DEBUG_DETAIL, "Override byte order int32_byte_order=%s", byte_order_str);

            rc = check_byte_order_str(byte_order_str, 4);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Byte order string int32_byte_order, \"%s\", is illegal or malformed.", byte_order_str);
                return rc;
            }

            tag->byte_order->int32_order[0] = (int)(unsigned int)(((unsigned int)byte_order_str[0] - (unsigned int)('0')) & 0x03);
            tag->byte_order->int32_order[1] = (int)(unsigned int)(((unsigned int)byte_order_str[1] - (unsigned int)('0')) & 0x03);
            tag->byte_order->int32_order[2] = (int)(unsigned int)(((unsigned int)byte_order_str[2] - (unsigned int)('0')) & 0x03);
            tag->byte_order->int32_order[3] = (int)(unsigned int)(((unsigned int)byte_order_str[3] - (unsigned int)('0')) & 0x03);
        }

        /* 64-bit ints. */
        byte_order_str = attr_get_str(attribs, "int64_byte_order", NULL);
        if(byte_order_str) {
            pdebug(DEBUG_DETAIL, "Override byte order int64_byte_order=%s", byte_order_str);

            rc = check_byte_order_str(byte_order_str, 8);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Byte order string int64_byte_order, \"%s\", is illegal or malformed.", byte_order_str);
                return rc;
            }

            tag->byte_order->int64_order[0] = (int)(unsigned int)(((unsigned int)byte_order_str[0] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[1] = (int)(unsigned int)(((unsigned int)byte_order_str[1] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[2] = (int)(unsigned int)(((unsigned int)byte_order_str[2] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[3] = (int)(unsigned int)(((unsigned int)byte_order_str[3] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[4] = (int)(unsigned int)(((unsigned int)byte_order_str[4] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[5] = (int)(unsigned int)(((unsigned int)byte_order_str[5] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[6] = (int)(unsigned int)(((unsigned int)byte_order_str[6] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[7] = (int)(unsigned int)(((unsigned int)byte_order_str[7] - (unsigned int)('0')) & 0x07);
        }

        /* 32-bit floats. */
        byte_order_str = attr_get_str(attribs, "float32_byte_order", NULL);
        if(byte_order_str) {
            pdebug(DEBUG_DETAIL, "Override byte order float32_byte_order=%s", byte_order_str);

            rc = check_byte_order_str(byte_order_str, 4);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Byte order string float32_byte_order, \"%s\", is illegal or malformed.", byte_order_str);
                return rc;
            }

            tag->byte_order->float32_order[0] = (int)(unsigned int)(((unsigned int)byte_order_str[0] - (unsigned int)('0')) & 0x03);
            tag->byte_order->float32_order[1] = (int)(unsigned int)(((unsigned int)byte_order_str[1] - (unsigned int)('0')) & 0x03);
            tag->byte_order->float32_order[2] = (int)(unsigned int)(((unsigned int)byte_order_str[2] - (unsigned int)('0')) & 0x03);
            tag->byte_order->float32_order[3] = (int)(unsigned int)(((unsigned int)byte_order_str[3] - (unsigned int)('0')) & 0x03);
        }

        /* 64-bit floats */
        byte_order_str = attr_get_str(attribs, "float64_byte_order", NULL);
        if(byte_order_str) {
            pdebug(DEBUG_DETAIL, "Override byte order float64_byte_order=%s", byte_order_str);

            rc = check_byte_order_str(byte_order_str, 8);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Byte order string float64_byte_order, \"%s\", is illegal or malformed.", byte_order_str);
                return rc;
            }

            tag->byte_order->float64_order[0] = (int)(unsigned int)(((unsigned int)byte_order_str[0] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[1] = (int)(unsigned int)(((unsigned int)byte_order_str[1] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[2] = (int)(unsigned int)(((unsigned int)byte_order_str[2] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[3] = (int)(unsigned int)(((unsigned int)byte_order_str[3] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[4] = (int)(unsigned int)(((unsigned int)byte_order_str[4] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[5] = (int)(unsigned int)(((unsigned int)byte_order_str[5] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[6] = (int)(unsigned int)(((unsigned int)byte_order_str[6] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[7] = (int)(unsigned int)(((unsigned int)byte_order_str[7] - (unsigned int)('0')) & 0x07);
        }

        /* string information. */

        /* is the string counted? */
        if(attr_get_str(attribs, "str_is_counted", NULL)) {
            str_param = attr_get_int(attribs, "str_is_counted", 0);
            if(str_param == 1 || str_param == 0) {
                tag->byte_order->str_is_counted = (str_param ? 1 : 0);
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_is_counted must be missing, zero (0) or one (1)!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* is the string a fixed length? */
        if(attr_get_str(attribs, "str_is_fixed_length", NULL)) {
            str_param = attr_get_int(attribs, "str_is_fixed_length", 0);
            if(str_param == 1 || str_param == 0) {
                tag->byte_order->str_is_fixed_length = (str_param ? 1: 0);
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_is_fixed_length must be missing, zero (0) or one (1)!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* is the string zero terminated? */
        if(attr_get_str(attribs, "str_is_zero_terminated", NULL)) {
            str_param = attr_get_int(attribs, "str_is_zero_terminated", 0);
            if(str_param == 1 || str_param == 0) {
                tag->byte_order->str_is_zero_terminated = (str_param ? 1 : 0);
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_is_zero_terminated must be missing, zero (0) or one (1)!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* is the string byteswapped like PLC/5? */
        if(attr_get_str(attribs, "str_is_byte_swapped", NULL)) {
            str_param = attr_get_int(attribs, "str_is_byte_swapped", 0);
            if(str_param == 1 || str_param == 0) {
                tag->byte_order->str_is_byte_swapped = (str_param ? 1 : 0);
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_is_byte_swapped must be missing, zero (0) or one (1)!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* main string parameters. */

        /* how many bytes is the string count word? */
        if(attr_get_str(attribs, "str_count_word_bytes", NULL)) {
            str_param = attr_get_int(attribs, "str_count_word_bytes", 0);
            if(str_param == 0 || str_param == 1 || str_param == 2 || str_param == 4 || str_param == 8) {
                tag->byte_order->str_count_word_bytes = (unsigned int)str_param;
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_count_word_bytes must be missing, 0, 1, 2, 4, or 8!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* What is the string maximum capacity */
        if(attr_get_str(attribs, "str_max_capacity", NULL)) {
            str_param = attr_get_int(attribs, "str_max_capacity", 0);
            if(str_param >= 0) {
                tag->byte_order->str_max_capacity = (unsigned int)str_param;
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_max_capacity must be missing, 0, or positive!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* What is the string total length */
        if(attr_get_str(attribs, "str_total_length", NULL)) {
            str_param = attr_get_int(attribs, "str_total_length", 0);
            if(str_param >= 0) {
                tag->byte_order->str_total_length = (unsigned int)str_param;
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_total_length must be missing, 0, or positive!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* What is the string padding length */
        if(attr_get_str(attribs, "str_pad_bytes", NULL)) {
            str_param = attr_get_int(attribs, "str_pad_bytes", 0);
            if(str_param >= 0) {
                tag->byte_order->str_pad_bytes = (unsigned int)str_param;
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_pad_bytes must be missing, 0, or positive!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* now make sure that the combination of settings works. */

        /* if we have a counted string, we need the count! */
        if(tag->byte_order->str_is_counted) {
            if(tag->byte_order->str_count_word_bytes == 0) {
                pdebug(DEBUG_WARN, "If a string definition is counted, you must use both \"str_is_counted\" and \"str_count_word_bytes\" parameters!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* if we have a fixed length string, we need to know what the length is! */
        if(tag->byte_order->str_is_fixed_length) {
            if(tag->byte_order->str_total_length == 0) {
                pdebug(DEBUG_WARN, "If a string definition is fixed length, you must use both \"str_is_fixed_length\" and \"str_total_length\" parameters!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* check the total length. */
        if(tag->byte_order->str_total_length > 0
          && (tag->byte_order->str_is_zero_terminated
            + tag->byte_order->str_max_capacity
            + tag->byte_order->str_count_word_bytes
            + tag->byte_order->str_pad_bytes)
          > tag->byte_order->str_total_length)
        {
            pdebug(DEBUG_WARN, "Tag string total length must be at least the sum of the other string components!");
            return PLCTAG_ERR_BAD_PARAM;
        }

        /* Do we have enough of a definition for a string? */
        /* FIXME - This is probably not enough checking! */
        if(tag->byte_order->str_is_counted || tag->byte_order->str_is_zero_terminated) {
            tag->byte_order->str_is_defined = 1;
        } else {
            pdebug(DEBUG_WARN, "Insufficient definitions found to support strings!");
        }
    }

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_OK;
}

int check_byte_order_str(const char *byte_order, int length)
{
    int taken[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    int byte_order_len = str_length(byte_order);

    pdebug(DEBUG_DETAIL, "Starting.");

    /* check the size. */
    if(byte_order_len != length) {
        pdebug(DEBUG_WARN, "Byte order string, \"%s\", must be %d characters long!", byte_order, length);
        return (byte_order_len < length ? PLCTAG_ERR_TOO_SMALL : PLCTAG_ERR_TOO_LARGE);
    }

    /* check each character. */
    for(int i=0; i < byte_order_len; i++) {
        int val = 0;

        if(!isdigit(byte_order[i]) || byte_order[i] < '0' || byte_order[i] > '7') {
            pdebug(DEBUG_WARN, "Byte order string, \"%s\", must be only characters from '0' to '7'!", byte_order);
            return PLCTAG_ERR_BAD_DATA;
        }

        /* get the numeric value. */
        val = byte_order[i] - '0';

        if(val < 0 || val > (length-1)) {
            pdebug(DEBUG_WARN, "Byte order string, \"%s\", must only values from 0 to %d!", byte_order, (length - 1));
            return PLCTAG_ERR_BAD_DATA;
        }

        if(taken[val]) {
            pdebug(DEBUG_WARN, "Byte order string, \"%s\", must use each digit exactly once!", byte_order);
            return PLCTAG_ERR_BAD_DATA;
        }

        taken[val] = 1;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}




plc_tag_p lookup_tag(int32_t tag_id)
{
    plc_tag_p tag = NULL;

    critical_block(tag_lookup_mutex) {
        tag = (plc_tag_p)hashtable_get(tags, (int64_t)tag_id);

        if(tag) {
            debug_set_tag_id(tag->tag_id);
        } else {
            /* TODO - remove this. */
            pdebug(DEBUG_WARN, "Tag with ID %d not found.", tag_id);
        }

        if(tag && tag->tag_id == tag_id) {
            pdebug(DEBUG_SPEW, "Found tag %p with id %d.", tag, tag->tag_id);
            tag = (plc_tag_p)rc_inc(tag);
        } else {
            debug_set_tag_id(0);
            tag = NULL;
        }
    }

    return tag;
}



int tag_id_inc(int id)
{
    if(id <= 0) {
        pdebug(DEBUG_ERROR, "Incoming ID is not valid! Got %d",id);
        /* try to correct. */
        id = (TAG_ID_MASK/2);
    }

    id = (id + 1) & TAG_ID_MASK;

    if(id == 0) {
        id = 1; /* skip zero intentionally! Can't return an ID of zero because it looks like a NULL pointer */
    }

    return id;
}



int add_tag_lookup(plc_tag_p tag)
{
    int rc = PLCTAG_ERR_NOT_FOUND;
    int new_id = 0;

    pdebug(DEBUG_DETAIL, "Starting.");

    critical_block(tag_lookup_mutex) {
        int attempts = 0;

        /* only get this when we hold the mutex. */
        new_id = next_tag_id;

        do {
            new_id = tag_id_inc(new_id);

            if(new_id <=0) {
                pdebug(DEBUG_WARN,"ID %d is illegal!", new_id);
                attempts = MAX_TAG_MAP_ATTEMPTS;
                break;
            }

            pdebug(DEBUG_SPEW,"Trying new ID %d.", new_id);

            if(!hashtable_get(tags,(int64_t)new_id)) {
                pdebug(DEBUG_DETAIL,"Found unused ID %d", new_id);
                break;
            }

            attempts++;
        } while(attempts < MAX_TAG_MAP_ATTEMPTS);

        if(attempts < MAX_TAG_MAP_ATTEMPTS) {
            rc = hashtable_put(tags, (int64_t)new_id, tag);
        } else {
            rc = PLCTAG_ERR_NO_RESOURCES;
        }

        next_tag_id = new_id;
    }

    if(rc != PLCTAG_STATUS_OK) {
        new_id = rc;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return new_id;
}





/*
 * get the string length depending on the PLC string type.
 *
 * This is called in other functions so is separated out.
 *
 * This must be called with the tag API mutex held!
 */

int get_string_length_unsafe(plc_tag_p tag, int offset)
{
    int string_length = 0;

    if(tag->byte_order->str_is_counted) {
        switch(tag->byte_order->str_count_word_bytes) {
            case 1:
                string_length = (int)(unsigned int)(tag->data[offset]);
                break;

            case 2:
                string_length = (int16_t)(uint16_t)(((uint16_t)(tag->data[offset + tag->byte_order->int16_order[0]]) << 0 ) +
                                                    ((uint16_t)(tag->data[offset + tag->byte_order->int16_order[1]]) << 8 ));
                break;

            case 4:
                string_length = (int32_t)(((uint32_t)(tag->data[offset + tag->byte_order->int32_order[0]]) << 0 ) +
                                          ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[1]]) << 8 ) +
                                          ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[2]]) << 16) +
                                          ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[3]]) << 24));
                break;

            default:
                pdebug(DEBUG_WARN, "Unsupported string count word size, %d bytes!", tag->byte_order->str_count_word_bytes);
                return 0; /* FIXME - this should be an error code. */
                break;
        }

    } else {
        if(tag->byte_order->str_is_zero_terminated) {
            /* slow, but hopefully correct. */

            /*
             * note that this will count the correct length of a string that runs up against
             * the end of the tag buffer.
             */
            for(int i = offset + (int)(tag->byte_order->str_count_word_bytes); i < tag->size; i++) {
                size_t char_index = (((size_t)(unsigned int)string_length) ^ (tag->byte_order->str_is_byte_swapped)) /* byte swap if necessary */
                                + (size_t)(unsigned int)offset
                                + (size_t)(unsigned int)(tag->byte_order->str_count_word_bytes);

                if(tag->data[char_index] == (uint8_t)0) {
                    /* found the end. */
                    break;
                }

                string_length++;
            }
        } else {
            /* it is not counted or zero terminated, so it is not supported. */
            pdebug(DEBUG_WARN, "Unsupported string length type.   Must be counted or zero-terminated!");
            return 0; /* FIXME this should be an error code. */
        }
    }

    return string_length;
}

#endif // __LIB_LIB_C__

