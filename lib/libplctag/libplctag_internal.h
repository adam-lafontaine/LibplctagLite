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

#ifndef __LIBPLCTAG_INTERNAL_H__
#define __LIBPLCTAG_INTERNAL_H__

#include "libplctag.h"


#ifdef __cplusplus
extern "C" {
#endif


#include <stddef.h>
#include <math.h>

#ifndef __PLATFORM_H__
#define __PLATFORM_H__

#ifdef _WIN32

#define _WINSOCKAPI_
#include <windows.h>
#include <tchar.h>
#include <strsafe.h>
#include <io.h>
#include <Winsock2.h>
#include <Ws2tcpip.h>
#include <string.h>
#include <stdlib.h>
#include <winnt.h>
#include <errno.h>
#include <math.h>
#include <process.h>
#include <time.h>
#include <stdio.h>

/*#include <WinSock2.h>*/
/*#include <Ws2tcpip.h>*/
/*#include <sys/types.h>*/
#include <stdint.h>
/*#include <io.h>*/
/*#include <stdlib.h>*/
#include <malloc.h>

//#include "libplctag_lib.h"



/* WinSock does not define this or support signals */
#define MSG_NOSIGNAL 0

#ifdef _MSC_VER
    /* MS Visual Studio C compiler. */
    #define START_PACK __pragma( pack(push, 1) )
    #define END_PACK   __pragma( pack(pop) )
    #define __PRETTY_FUNCTION__ __FUNCTION__
#else
    /* MinGW on Windows. */
    #define START_PACK
    #define END_PACK  __attribute__((packed))
    #define __PRETTY_FUNCTION__  __func__
#endif

/* VS C++ uses foo[] to denote a zero length array. */
#define ZLA_SIZE

/* export definitions. */

#define USE_STD_VARARG_MACROS 1

/* Apparently ssize_t is not on Windows. */
#if defined(_MSC_VER)
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
#endif


#ifndef COUNT_NARG
#define COUNT_NARG(...)                                                \
         COUNT_NARG_(__VA_ARGS__,COUNT_RSEQ_N())
#endif

#ifndef COUNT_NARG_
#define COUNT_NARG_(...)                                               \
         COUNT_ARG_N(__VA_ARGS__)
#endif

#ifndef COUNT_ARG_N
#define COUNT_ARG_N(                                                   \
          _1, _2, _3, _4, _5, _6, _7, _8, _9,_10, \
         _11,_12,_13,_14,_15,_16,_17,_18,_19,_20, \
         _21,_22,_23,_24,_25,_26,_27,_28,_29,_30, \
         _31,_32,_33,_34,_35,_36,_37,_38,_39,_40, \
         _41,_42,_43,_44,_45,_46,_47,_48,_49,_50, \
         _51,_52,_53,_54,_55,_56,_57,_58,_59,_60, \
         _61,_62,_63,N,...) N
#endif

#ifndef COUNT_RSEQ_N
#define COUNT_RSEQ_N()                                                 \
         63,62,61,60,                   \
         59,58,57,56,55,54,53,52,51,50, \
         49,48,47,46,45,44,43,42,41,40, \
         39,38,37,36,35,34,33,32,31,30, \
         29,28,27,26,25,24,23,22,21,20, \
         19,18,17,16,15,14,13,12,11,10, \
         9,8,7,6,5,4,3,2,1,0
#endif



/* memory functions/defs */
void *mem_alloc(int size);
void *mem_realloc(void *orig, int size);
void mem_free(const void *mem);
void mem_set(void *d1, int c, int size);
void mem_copy(void *dest, void *src, int size);
void mem_move(void *dest, void *src, int size);
int mem_cmp(void *src1, int src1_size, void *src2, int src2_size);

/* string functions/defs */
int str_cmp(const char *first, const char *second);
int str_cmp_i(const char *first, const char *second);
int str_cmp_i_n(const char *first, const char *second, int num_chars);
char* str_str_cmp_i(const char* haystack, const char* needle);
int str_copy(char *dst, int dst_size, const char *src);
int str_length(const char *str);
char *str_dup(const char *str);
int str_to_int(const char *str, int *val);
int str_to_float(const char *str, float *val);
char **str_split(const char *str, const char *sep);
#define str_concat(s1, ...) str_concat_impl(COUNT_NARG(__VA_ARGS__)+1, s1, __VA_ARGS__)
char *str_concat_impl(int num_args, ...);

/* mutex functions/defs */
typedef struct mutex_t *mutex_p;
int mutex_create(mutex_p *m);
// int mutex_lock(mutex_p m);
// int mutex_try_lock(mutex_p m);
// int mutex_unlock(mutex_p m);
int mutex_destroy(mutex_p *m);

int mutex_lock_impl(const char *func, int line_num, mutex_p m);
int mutex_try_lock_impl(const char *func, int line_num, mutex_p m);
int mutex_unlock_impl(const char *func, int line_num, mutex_p m);

#if defined(_WIN32) && defined(_MSC_VER)
    /* MinGW on Windows does not need this. */
    #define __func__ __FUNCTION__
#endif

#define mutex_lock(m) mutex_lock_impl(__func__, __LINE__, m)
#define mutex_try_lock(m) mutex_try_lock_impl(__func__, __LINE__, m)
#define mutex_unlock(m) mutex_unlock_impl(__func__, __LINE__, m)

/* macros are evil */

/*
 * Use this one like this:
 *
 *     critical_block(my_mutex) {
 *         locked_data++;
 *         foo(locked_data);
 *     }
 *
 * The macro locks and unlocks for you.  Derived from ideas/code on StackOverflow.com.
 *
 * Do not use break, return, goto or continue inside the synchronized block if
 * you intend to have them apply to a loop outside the synchronized block.
 *
 * You can use break, but it will drop out of the inner for loop and correctly
 * unlock the mutex.  It will NOT break out of any surrounding loop outside the
 * synchronized block.
 */

#define PLCTAG_CAT2(a,b) a##b
#define PLCTAG_CAT(a,b) PLCTAG_CAT2(a,b)
#define LINE_ID(base) PLCTAG_CAT(base,__LINE__)

#define critical_block(lock) \
for(int LINE_ID(__sync_flag_nargle_) = 1; LINE_ID(__sync_flag_nargle_); LINE_ID(__sync_flag_nargle_) = 0, mutex_unlock(lock))  for(int LINE_ID(__sync_rc_nargle_) = mutex_lock(lock); LINE_ID(__sync_rc_nargle_) == PLCTAG_STATUS_OK && LINE_ID(__sync_flag_nargle_) ; LINE_ID(__sync_flag_nargle_) = 0)

/* thread functions/defs */
typedef struct thread_t *thread_p;
//typedef PTHREAD_START_ROUTINE thread_func_t;
//typedef DWORD /*WINAPI*/ (*thread_func_t)(void *lpParam );
int thread_create(thread_p *t, LPTHREAD_START_ROUTINE func, int stacksize, void *arg);
void thread_stop(void);
void thread_kill(thread_p t);
int thread_join(thread_p t);
int thread_detach();
int thread_destroy(thread_p *t);

#define THREAD_FUNC(func) DWORD __stdcall func(LPVOID arg)
#define THREAD_RETURN(val) return (DWORD)val;

#define THREAD_LOCAL __declspec(thread)

/* atomic operations */
#define spin_block(lock) \
for(int LINE_ID(__sync_flag_nargle_lock) = 1; LINE_ID(__sync_flag_nargle_lock); LINE_ID(__sync_flag_nargle_lock) = 0, lock_release(lock))  for(int LINE_ID(__sync_rc_nargle_lock) = lock_acquire(lock); LINE_ID(__sync_rc_nargle_lock) && LINE_ID(__sync_flag_nargle_lock) ; LINE_ID(__sync_flag_nargle_lock) = 0)

typedef volatile long int lock_t;

#define LOCK_INIT (0)

/* returns non-zero when lock acquired, zero when lock operation failed */
int lock_acquire_try(lock_t *lock);
int lock_acquire(lock_t *lock);
void lock_release(lock_t *lock);


/* condition variables */
typedef struct cond_t* cond_p;
int cond_create(cond_p * c);
int cond_wait_impl(const char *func, int line_num, cond_p c, int timeout_ms);
int cond_signal_impl(const char *func, int line_num, cond_p c);
int cond_clear_impl(const char *func, int line_num, cond_p c);
int cond_destroy(cond_p *c);

#define cond_wait(c, t) cond_wait_impl(__func__, __LINE__, c, t)
#define cond_signal(c) cond_signal_impl(__func__, __LINE__, c)
#define cond_clear(c) cond_clear_impl(__func__, __LINE__, c)

/* socket functions */
typedef struct sock_t *sock_p;
typedef enum {
    SOCK_EVENT_NONE         = 0,
    SOCK_EVENT_TIMEOUT      = (1 << 0),
    SOCK_EVENT_DISCONNECT   = (1 << 1),
    SOCK_EVENT_ERROR        = (1 << 2),
    SOCK_EVENT_CAN_READ     = (1 << 3),
    SOCK_EVENT_CAN_WRITE    = (1 << 4),
    SOCK_EVENT_WAKE_UP      = (1 << 5),
    SOCK_EVENT_CONNECT      = (1 << 6),

    SOCK_EVENT_DEFAULT_MASK = (SOCK_EVENT_TIMEOUT | SOCK_EVENT_DISCONNECT | SOCK_EVENT_ERROR | SOCK_EVENT_WAKE_UP)
} sock_event_t;
int socket_create(sock_p *s);
int socket_connect_tcp_start(sock_p s, const char *host, int port);
int socket_connect_tcp_check(sock_p s, int timeout_ms);
int socket_wait_event(sock_p sock, int events, int timeout_ms);
int socket_wake(sock_p sock);
int socket_read(sock_p s, uint8_t *buf, int size, int timeout_ms);
int socket_write(sock_p s, uint8_t *buf, int size, int timeout_ms);
int socket_close(sock_p s);
int socket_destroy(sock_p *s);


/* serial handling */
typedef struct serial_port_t *serial_port_p;
#define PLC_SERIAL_PORT_NULL ((plc_serial_port)NULL)
serial_port_p plc_lib_open_serial_port(const char *path, int baud_rate, int data_bits, int stop_bits, int parity_type);
int plc_lib_close_serial_port(serial_port_p serial_port);
int plc_lib_serial_port_read(serial_port_p serial_port, uint8_t *data, int size);
int plc_lib_serial_port_write(serial_port_p serial_port, uint8_t *data, int size);


/* time functions */
int sleep_ms(int ms);
int64_t time_ms(void);
struct tm *localtime_r(const time_t *timep, struct tm *result);

/* some functions can be simply replaced */
#define snprintf_platform sprintf_s

#else

#include <stddef.h>
#include <stdint.h>
#include <math.h>
#include <stdarg.h>

/* common definitions */
#define START_PACK
#define END_PACK __attribute__((__packed__))

#define ZLA_SIZE 0

#define USE_GNU_VARARG_MACROS 1

#ifndef COUNT_NARG
#define COUNT_NARG(...)                                                \
         COUNT_NARG_(__VA_ARGS__,COUNT_RSEQ_N())
#endif

#ifndef COUNT_NARG_
#define COUNT_NARG_(...)                                               \
         COUNT_ARG_N(__VA_ARGS__)
#endif

#ifndef COUNT_ARG_N
#define COUNT_ARG_N(                                                   \
          _1, _2, _3, _4, _5, _6, _7, _8, _9,_10, \
         _11,_12,_13,_14,_15,_16,_17,_18,_19,_20, \
         _21,_22,_23,_24,_25,_26,_27,_28,_29,_30, \
         _31,_32,_33,_34,_35,_36,_37,_38,_39,_40, \
         _41,_42,_43,_44,_45,_46,_47,_48,_49,_50, \
         _51,_52,_53,_54,_55,_56,_57,_58,_59,_60, \
         _61,_62,_63,N,...) N
#endif

#ifndef COUNT_RSEQ_N
#define COUNT_RSEQ_N()                                                 \
         63,62,61,60,                   \
         59,58,57,56,55,54,53,52,51,50, \
         49,48,47,46,45,44,43,42,41,40, \
         39,38,37,36,35,34,33,32,31,30, \
         29,28,27,26,25,24,23,22,21,20, \
         19,18,17,16,15,14,13,12,11,10, \
         9,8,7,6,5,4,3,2,1,0
#endif


/* memory functions/defs */
void *mem_alloc(int size);
void *mem_realloc(void *orig, int size);
void mem_free(const void *mem);
void mem_set(void *dest, int c, int size);
void mem_copy(void *dest, void *src, int size);
void mem_move(void *dest, void *src, int size);
int mem_cmp(void *src1, int src1_size, void *src2, int src2_size);

/* string functions/defs */
int str_cmp(const char *first, const char *second);
int str_cmp_i(const char *first, const char *second);
int str_cmp_i_n(const char *first, const char *second, int num_chars);
char *str_str_cmp_i(const char *haystack, const char *needle);
int str_copy(char *dst, int dst_size, const char *src);
int str_length(const char *str);
char *str_dup(const char *str);
int str_to_int(const char *str, int *val);
int str_to_float(const char *str, float *val);
char **str_split(const char *str, const char *sep);
#define str_concat(s1, ...) str_concat_impl(COUNT_NARG(__VA_ARGS__)+1, s1, __VA_ARGS__)
char *str_concat_impl(int num_args, ...);

/* mutex functions/defs */
typedef struct mutex_t *mutex_p;
int mutex_create(mutex_p *m);
// int mutex_lock(mutex_p m);
// int mutex_try_lock(mutex_p m);
// int mutex_unlock(mutex_p m);
int mutex_destroy(mutex_p *m);

int mutex_lock_impl(const char *func, int line_num, mutex_p m);
int mutex_try_lock_impl(const char *func, int line_num, mutex_p m);
int mutex_unlock_impl(const char *func, int line_num, mutex_p m);

#if defined(_WIN32) && defined(_MSC_VER)
    /* MinGW on Windows does not need this. */
    #define __func__ __FUNCTION__
#endif

#define mutex_lock(m) mutex_lock_impl(__func__, __LINE__, m)
#define mutex_try_lock(m) mutex_try_lock_impl(__func__, __LINE__, m)
#define mutex_unlock(m) mutex_unlock_impl(__func__, __LINE__, m)

/* macros are evil */

/*
 * Use this one like this:
 *
 *     critical_block(my_mutex) {
 *         locked_data++;
 *         foo(locked_data);
 *     }
 *
 * The macro locks and unlocks for you.  Derived from ideas/code on StackOverflow.com.
 *
 * Do not use break, return, goto or continue inside the synchronized block if
 * you intend to have them apply to a loop outside the synchronized block.
 *
 * You can use break, but it will drop out of the inner for loop and correctly
 * unlock the mutex.  It will NOT break out of any surrounding loop outside the
 * synchronized block.
 */
#define critical_block(lock) \
for(int __sync_flag_nargle_##__LINE__ = 1; __sync_flag_nargle_##__LINE__ ; __sync_flag_nargle_##__LINE__ = 0, mutex_unlock(lock))  for(int __sync_rc_nargle_##__LINE__ = mutex_lock(lock); __sync_rc_nargle_##__LINE__ == PLCTAG_STATUS_OK && __sync_flag_nargle_##__LINE__ ; __sync_flag_nargle_##__LINE__ = 0)

/* thread functions/defs */
typedef struct thread_t *thread_p;
typedef void *(*thread_func_t)(void *arg);
int thread_create(thread_p *t, thread_func_t func, int stacksize, void *arg);
void thread_stop(void) __attribute__((noreturn));
void thread_kill(thread_p t);
int thread_join(thread_p t);
int thread_detach();
int thread_destroy(thread_p *t);

#define THREAD_FUNC(func) void *func(void *arg)
#define THREAD_RETURN(val) return (void *)val;

#define THREAD_LOCAL __thread

/* atomic operations */
#define spin_block(lock) \
for(int __sync_flag_nargle_lock_##__LINE__ = 1; __sync_flag_nargle_lock_##__LINE__ ; __sync_flag_nargle_lock_##__LINE__ = 0, lock_release(lock))  for(int __sync_rc_nargle_lock_##__LINE__ = lock_acquire(lock); __sync_rc_nargle_lock_##__LINE__ && __sync_flag_nargle_lock_##__LINE__ ; __sync_flag_nargle_lock_##__LINE__ = 0)

typedef int lock_t;

#define LOCK_INIT (0)

/* returns non-zero when lock acquired, zero when lock operation failed */
int lock_acquire_try(lock_t *lock);
int lock_acquire(lock_t *lock);
void lock_release(lock_t *lock);


/* condition variables */
typedef struct cond_t *cond_p;
int cond_create(cond_p *c);
int cond_wait_impl(const char *func, int line_num, cond_p c, int timeout_ms);
int cond_signal_impl(const char *func, int line_num, cond_p c);
int cond_clear_impl(const char *func, int line_num, cond_p c);
int cond_destroy(cond_p *c);

#define cond_wait(c, t) cond_wait_impl(__func__, __LINE__, c, t)
#define cond_signal(c) cond_signal_impl(__func__, __LINE__, c)
#define cond_clear(c) cond_clear_impl(__func__, __LINE__, c)


/* socket functions */
typedef struct sock_t *sock_p;
typedef enum {
    SOCK_EVENT_NONE         = 0,
    SOCK_EVENT_TIMEOUT      = (1 << 0),
    SOCK_EVENT_DISCONNECT   = (1 << 1),
    SOCK_EVENT_ERROR        = (1 << 2),
    SOCK_EVENT_CAN_READ     = (1 << 3),
    SOCK_EVENT_CAN_WRITE    = (1 << 4),
    SOCK_EVENT_WAKE_UP      = (1 << 5),
    SOCK_EVENT_CONNECT      = (1 << 6),

    SOCK_EVENT_DEFAULT_MASK = (SOCK_EVENT_TIMEOUT | SOCK_EVENT_DISCONNECT | SOCK_EVENT_ERROR | SOCK_EVENT_WAKE_UP )
} sock_event_t;
int socket_create(sock_p *s);
int socket_connect_tcp_start(sock_p s, const char *host, int port);
int socket_connect_tcp_check(sock_p s, int timeout_ms);
int socket_wait_event(sock_p sock, int events, int timeout_ms);
int socket_wake(sock_p sock);
int socket_read(sock_p s, uint8_t *buf, int size, int timeout_ms);
int socket_write(sock_p s, uint8_t *buf, int size, int timeout_ms);
int socket_close(sock_p s);
int socket_destroy(sock_p *s);

/* serial handling */
/* FIXME - either implement this or remove it. */
typedef struct serial_port_t *serial_port_p;
#define PLC_SERIAL_PORT_NULL ((plc_serial_port)NULL)
serial_port_p plc_lib_open_serial_port(const char *path, int baud_rate, int data_bits, int stop_bits, int parity_type);
int plc_lib_close_serial_port(serial_port_p serial_port);
int plc_lib_serial_port_read(serial_port_p serial_port, uint8_t *data, int size);
int plc_lib_serial_port_write(serial_port_p serial_port, uint8_t *data, int size);

/* misc functions */
int sleep_ms(int ms);
int64_t time_ms(void);

#define snprintf_platform snprintf

#endif // _WIN32

#endif // __PLATFORM_H__


#ifndef __UTIL_ATOMIC_H__
#define __UTIL_ATOMIC_H__

typedef struct { lock_t lock; volatile int val; } atomic_int;

void atomic_init(atomic_int *a, int new_val);
int atomic_get(atomic_int *a);
int atomic_set(atomic_int *a, int new_val);
int atomic_add(atomic_int *a, int other);
int atomic_compare_and_set(atomic_int *a, int old_val, int new_val);

#endif // __UTIL_ATOMIC_H__


#ifndef __UTIL_ATTR_H__
#define __UTIL_ATTR_H__

typedef struct attr_entry_t *attr_entry;
typedef struct attr_t *attr;

attr_entry find_entry(attr a, const char *name);
attr attr_create(void);
attr attr_create_from_str(const char *attr_str);
int attr_set_str(attr attrs, const char *name, const char *val);
int attr_set_int(attr attrs, const char *name, int val);
int attr_set_float(attr attrs, const char *name, float val);
const char *attr_get_str(attr attrs, const char *name, const char *def);
int attr_get_int(attr attrs, const char *name, int def);
float attr_get_float(attr attrs, const char *name, float def);
int attr_remove(attr attrs, const char *name);
void attr_destroy(attr attrs);

#endif // __UTIL_ATTR_H__


#ifndef __UTIL_BYTEORDER_H__
#define __UTIL_BYTEORDER_H__

START_PACK typedef struct {
    union {
        uint8_t b_val[2];
        uint16_t u_val;
    } val;
} END_PACK uint16_le;

#define UINT16_LE_INIT(v) {.val = {.u_val = ((uint16_t)(v))}}

START_PACK typedef struct {
    union {
        uint8_t b_val[4];
        uint32_t u_val;
    } val;
} END_PACK uint32_le;

#define UINT32_LE_INIT(v) {.val = {.u_val = ((uint32_t)(v))}}


START_PACK typedef struct {
    union {
        uint8_t b_val[8];
        uint64_t u_val;
    } val;
} END_PACK uint64_le;

#define UINT64_LE_INIT(v) {.val = {.u_val = ((uint64_t)(v))}}



inline static uint16_le h2le16(uint16_t val)
{
    uint16_le result;

    result.val.b_val[0] = (uint8_t)(val & 0xFF);
    result.val.b_val[1] = (uint8_t)((val >> 8) & 0xFF);

    return result;
}


inline static uint16_t le2h16(uint16_le src)
{
    uint16_t result = 0;

    result = (uint16_t)(src.val.b_val[0] + ((src.val.b_val[1]) << 8));

    return result;
}




inline static uint32_le h2le32(uint32_t val)
{
    uint32_le result;

    result.val.b_val[0] = (uint8_t)(val & 0xFF);
    result.val.b_val[1] = (uint8_t)((val >> 8) & 0xFF);
    result.val.b_val[2] = (uint8_t)((val >> 16) & 0xFF);
    result.val.b_val[3] = (uint8_t)((val >> 24) & 0xFF);

    return result;
}


inline static uint32_t le2h32(uint32_le src)
{
    uint32_t result = 0;

    result |= (uint32_t)(src.val.b_val[0]);
    result |= ((uint32_t)(src.val.b_val[1]) << 8);
    result |= ((uint32_t)(src.val.b_val[2]) << 16);
    result |= ((uint32_t)(src.val.b_val[3]) << 24);

    return result;
}






inline static uint64_le h2le64(uint64_t val)
{
    uint64_le result;

    result.val.b_val[0] = (uint8_t)(val & 0xFF);
    result.val.b_val[1] = (uint8_t)((val >> 8) & 0xFF);
    result.val.b_val[2] = (uint8_t)((val >> 16) & 0xFF);
    result.val.b_val[3] = (uint8_t)((val >> 24) & 0xFF);
    result.val.b_val[4] = (uint8_t)((val >> 32) & 0xFF);
    result.val.b_val[5] = (uint8_t)((val >> 40) & 0xFF);
    result.val.b_val[6] = (uint8_t)((val >> 48) & 0xFF);
    result.val.b_val[7] = (uint8_t)((val >> 56) & 0xFF);

    return result;
}


inline static uint64_t le2h64(uint64_le src)
{
    uint64_t result = 0;

    result |= (uint64_t)(src.val.b_val[0]);
    result |= ((uint64_t)(src.val.b_val[1]) << 8);
    result |= ((uint64_t)(src.val.b_val[2]) << 16);
    result |= ((uint64_t)(src.val.b_val[3]) << 24);
    result |= ((uint64_t)(src.val.b_val[4]) << 32);
    result |= ((uint64_t)(src.val.b_val[5]) << 40);
    result |= ((uint64_t)(src.val.b_val[6]) << 48);
    result |= ((uint64_t)(src.val.b_val[7]) << 56);

    return result;
}



/* as seen on comp.arch by David Brown

uint32_t read32be(const uint8_t * buf) {
    uint32_t res = 0;
    res = (res << 8) | *buf++;
    res = (res << 8) | *buf++;
    res = (res << 8) | *buf++;
    res = (res << 8) | *buf++;
    return res;
}


uint32_t read32le(const uint8_t * p) {
    uint32_t x = 0;
    x = (x >> 8) | ((uint32_t) *p++ << 24);
    x = (x >> 8) | ((uint32_t) *p++ << 24);
    x = (x >> 8) | ((uint32_t) *p++ << 24);
    x = (x >> 8) | ((uint32_t) *p++ << 24);
    return x;
}

*/

#endif // __UTIL_BYTEORDER_H__


#ifndef __UTIL_DEBUG_H__
#define __UTIL_DEBUG_H__

#define DEBUG_NONE      (0)
#define DEBUG_ERROR     (1)
#define DEBUG_WARN      (2)
#define DEBUG_INFO      (3)
#define DEBUG_DETAIL    (4)
#define DEBUG_SPEW      (5)
#define DEBUG_END       (6)

int set_debug_level(int debug_level);
int get_debug_level(void);
void debug_set_tag_id(int32_t tag_id);

void pdebug_impl(const char *func, int line_num, int debug_level, const char *templ, ...);

#if defined(_WIN32) && defined(_MSC_VER)
    /* MinGW on Windows does not need this. */
    #define __func__ __FUNCTION__
#endif


#define pdebug(dbg,...)                                                \
   do { if((dbg) != DEBUG_NONE && (dbg) <= get_debug_level()) pdebug_impl(__func__, __LINE__, dbg, __VA_ARGS__); } while(0)

void pdebug_dump_bytes_impl(const char *func, int line_num, int debug_level, uint8_t *data,int count);
#define pdebug_dump_bytes(dbg, d,c)  do { if((dbg) != DEBUG_NONE && (dbg) <= get_debug_level()) pdebug_dump_bytes_impl(__func__, __LINE__,dbg,d,c); } while(0)

int debug_register_logger(void (*log_callback_func)(int32_t tag_id, int debug_level, const char *message));
int debug_unregister_logger(void);

#endif // __UTIL_DEBUG_H__


#ifndef __UTIL_HASH_H__
#define __UTIL_HASH_H__

uint32_t hash(uint8_t *k, size_t length, uint32_t initval);

#endif // __UTIL_HASH_H__


#ifndef __UTIL_HASHTABLE_H__
#define __UTIL_HASHTABLE_H__

typedef struct hashtable_t *hashtable_p;

hashtable_p hashtable_create(int size);
void *hashtable_get(hashtable_p table, int64_t key);
int hashtable_put(hashtable_p table, int64_t key, void *arg);
void *hashtable_get_index(hashtable_p table, int index);
int hashtable_capacity(hashtable_p table);
int hashtable_entries(hashtable_p table);
int hashtable_on_each(hashtable_p table, int (*callback_func)(hashtable_p table, int64_t key, void *data, void *context), void *context);
void *hashtable_remove(hashtable_p table, int64_t key);
int hashtable_destroy(hashtable_p table);

#endif // __UTIL_HASHTABLE_H__


#ifndef __MACROS_H__
#define __MACROS_H__

/*
 * This is a collection of handy macros.
 */

/* select one out of a list */
#define GET_MACRO(_1,_2,_3,_4,_5,_6,_7,_8,_9,_10,_11,_12,_13,_14,_15,_16,NAME,...) NAME

#define FOREACH_PAIR_2(WHAT, WHAT_LAST, X, Y) WHAT_LAST(X,Y)
#define FOREACH_PAIR_4(WHAT, WHAT_LAST, X, Y, ...) WHAT(X,Y)FOREACH_PAIR_2(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_PAIR_6(WHAT, WHAT_LAST, X, Y, ...) WHAT(X,Y)FOREACH_PAIR_4(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_PAIR_8(WHAT, WHAT_LAST, X, Y, ...) WHAT(X,Y)FOREACH_PAIR_6(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_PAIR_10(WHAT, WHAT_LAST, X, Y, ...) WHAT(X,Y)FOREACH_PAIR_8(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_PAIR_12(WHAT, WHAT_LAST, X, Y, ...) WHAT(X,Y)FOREACH_PAIR_10(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_PAIR_14(WHAT, WHAT_LAST, X, Y, ...) WHAT(X,Y)FOREACH_PAIR_12(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_PAIR_16(WHAT, WHAT_LAST, X, Y, ...) WHAT(X,Y)FOREACH_PAIR_14(WHAT, WHAT_LAST, __VA_ARGS__)

/* run an action macro against all elements passed as arguments, handles arguments in PAIRS */
#define FOREACH_PAIR(action, ...) \
  GET_MACRO(__VA_ARGS__,FOREACH_PAIR_16,OOPS15,FOREACH_PAIR_14,OOPS13,FOREACH_PAIR_12,OOPS11,FOREACH_PAIR_10,OOPS9,FOREACH_PAIR_8,OOPS7,FOREACH_PAIR_6,OOPS5,FOREACH_PAIR_4,OOPS3,FOREACH_PAIR_2,OOPS1,)(action,action,__VA_ARGS__)

/* as above, but run a second macro function against the last element in the list */
#define FOREACH_PAIR_LAST(action, action_last, ...) \
  GET_MACRO(__VA_ARGS__,FOREACH_PAIR_16,OOPS15,FOREACH_PAIR_14,OOPS13,FOREACH_PAIR_12,OOPS11,FOREACH_PAIR_10,OOPS9,FOREACH_PAIR_8,OOPS7,FOREACH_PAIR_6,OOPS5,FOREACH_PAIR_4,OOPS3,FOREACH_PAIR_2,OOPS1,)(action,action_last,__VA_ARGS__)


#define FOREACH_1(WHAT, WHAT_LAST, X) WHAT_LAST(X)
#define FOREACH_2(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_1(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_3(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_2(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_4(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_3(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_5(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_4(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_6(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_5(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_7(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_6(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_8(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_7(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_9(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_8(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_10(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_9(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_11(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_10(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_12(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_11(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_13(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_12(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_14(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_13(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_15(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_14(WHAT, WHAT_LAST, __VA_ARGS__)
#define FOREACH_16(WHAT, WHAT_LAST, X, ...) WHAT(X)FOREACH_15(WHAT, WHAT_LAST, __VA_ARGS__)


/* run action macro against all elements in a list. */
#define FOREACH(action, ...) \
   GET_MACRO(__VA_ARGS__,FOREACH_16,FOREACH_15,FOREACH_14,FOREACH_13,FOREACH_12,FOREACH_11,FOREACH_10,FOREACH_9,FOREACH_8,FOREACH_7,FOREACH_6,FOREACH_5,FOREACH_4,FOREACH_3,FOREACH_2,FOREACH_1,)(action,action,__VA_ARGS__)

/* run action macro against all elements in a list. Run a different macro against the last element. */
#define FOREACH_LAST(action, action_last, ...) \
   GET_MACRO(__VA_ARGS__,FOREACH_16,FOREACH_15,FOREACH_14,FOREACH_13,FOREACH_12,FOREACH_11,FOREACH_10,FOREACH_9,FOREACH_8,FOREACH_7,FOREACH_6,FOREACH_5,FOREACH_4,FOREACH_3,FOREACH_2,FOREACH_1,)(action,action_last,__VA_ARGS__)

/* count args */
#define COUNT_NARG(...)                                                \
         COUNT_NARG_(__VA_ARGS__,COUNT_RSEQ_N())

#define COUNT_NARG_(...)                                               \
         COUNT_ARG_N(__VA_ARGS__)

#define COUNT_ARG_N(                                                   \
          _1, _2, _3, _4, _5, _6, _7, _8, _9,_10, \
         _11,_12,_13,_14,_15,_16,_17,_18,_19,_20, \
         _21,_22,_23,_24,_25,_26,_27,_28,_29,_30, \
         _31,_32,_33,_34,_35,_36,_37,_38,_39,_40, \
         _41,_42,_43,_44,_45,_46,_47,_48,_49,_50, \
         _51,_52,_53,_54,_55,_56,_57,_58,_59,_60, \
         _61,_62,_63,N,...) N

#define COUNT_RSEQ_N()                                                 \
         63,62,61,60,                   \
         59,58,57,56,55,54,53,52,51,50, \
         49,48,47,46,45,44,43,42,41,40, \
         39,38,37,36,35,34,33,32,31,30, \
         29,28,27,26,25,24,23,22,21,20, \
         19,18,17,16,15,14,13,12,11,10, \
         9,8,7,6,5,4,3,2,1,0


#define container_of(ptr, type, member) ((type *)((char *)(1 ? (ptr) : &((type *)0)->member) - offsetof(type, member)))

#endif // __MACROS_H__


#ifndef __UTIL_RC_H__
#define __UTIL_RC_H__

typedef void (*rc_cleanup_func)(void *);

#define rc_alloc(size, cleaner) rc_alloc_impl(__func__, __LINE__, size, cleaner)
void *rc_alloc_impl(const char *func, int line_num, int size, rc_cleanup_func cleaner);

#define rc_inc(ref) rc_inc_impl(__func__, __LINE__, ref)
void *rc_inc_impl(const char *func, int line_num, void *ref);

#define rc_dec(ref) rc_dec_impl(__func__, __LINE__, ref)
void *rc_dec_impl(const char *func, int line_num, void *ref);

#endif // __UTIL_RC_H__


#ifndef __UTIL_VECTOR_H__
#define __UTIL_VECTOR_H__

typedef struct vector_t *vector_p;

vector_p vector_create(int capacity, int max_inc);
int vector_length(vector_p vec);
int vector_put(vector_p vec, int index, void * ref);
void *vector_get(vector_p vec, int index);
//int vector_on_each(vector_p vec, int (*callback_func)(vector_p vec, int index, void **data, int arg_count, void **args), int num_args, ...);
void *vector_remove(vector_p vec, int index);
int vector_destroy(vector_p vec);

#endif // __UTIL_VECTOR_H__


#ifndef __LIB_TAG_H__
#define __LIB_TAG_H__

// #define PLCTAG_CANARY (0xACA7CAFE)
// #define PLCTAG_DATA_LITTLE_ENDIAN   (0)
// #define PLCTAG_DATA_BIG_ENDIAN      (1)


typedef struct plc_tag_t *plc_tag_p;


typedef int (*tag_vtable_func)(plc_tag_p tag);

/* we'll need to set these per protocol type. */
struct tag_vtable_t {
    tag_vtable_func abort;
    tag_vtable_func read;
    tag_vtable_func status;
    tag_vtable_func tickler;
    tag_vtable_func write;

    tag_vtable_func wake_plc;

    /* attribute accessors. */
    int (*get_int_attrib)(plc_tag_p tag, const char *attrib_name, int default_value);
    int (*set_int_attrib)(plc_tag_p tag, const char *attrib_name, int new_value);
};

typedef struct tag_vtable_t *tag_vtable_p;


/* byte ordering */

struct tag_byte_order_s {
    /* set if we allocated this specifically for the tag. */
    unsigned int is_allocated:1;

    /* string type and ordering. */
    unsigned int str_is_defined:1;
    unsigned int str_is_counted:1;
    unsigned int str_is_fixed_length:1;
    unsigned int str_is_zero_terminated:1;
    unsigned int str_is_byte_swapped:1;

    unsigned int str_count_word_bytes;
    unsigned int str_max_capacity;
    unsigned int str_total_length;
    unsigned int str_pad_bytes;

    int int16_order[2];
    int int32_order[4];
    int int64_order[8];

    int float32_order[4];
    int float64_order[8];
};


typedef struct tag_byte_order_s tag_byte_order_t;


typedef void (*tag_callback_func)(int32_t tag_id, int event, int status);
typedef void (*tag_extended_callback_func)(int32_t tag_id, int event, int status, void *user_data);

/*
 * The base definition of the tag structure.  This is used
 * by the protocol-specific implementations.
 *
 * The base type only has a vtable for operations.
 */

#define TAG_BASE_STRUCT uint8_t is_bit:1; \
                        uint8_t tag_is_dirty:1; \
                        uint8_t read_in_flight:1; \
                        uint8_t read_complete:1; \
                        uint8_t write_in_flight:1; \
                        uint8_t write_complete:1; \
                        uint8_t skip_tickler:1; \
                        uint8_t had_created_event:1; \
                        uint8_t event_creation_complete:1; \
                        uint8_t event_deletion_started:1; \
                        uint8_t event_operation_aborted:1; \
                        uint8_t event_read_started: 1; \
                        uint8_t event_read_complete_enable: 1; \
                        uint8_t event_read_complete: 1; \
                        uint8_t event_write_started: 1; \
                        uint8_t event_write_complete_enable: 1; \
                        uint8_t event_write_complete: 1; \
                        int8_t event_creation_complete_status; \
                        int8_t event_deletion_started_status; \
                        int8_t event_operation_aborted_status; \
                        int8_t event_read_started_status; \
                        int8_t event_read_complete_status; \
                        int8_t event_write_started_status; \
                        int8_t event_write_complete_status; \
                        int8_t status; \
                        int bit; \
                        int connection_group_id; \
                        int32_t size; \
                        int32_t tag_id; \
                        int32_t auto_sync_read_ms; \
                        int32_t auto_sync_write_ms; \
                        uint8_t *data; \
                        tag_byte_order_t *byte_order; \
                        mutex_p ext_mutex; \
                        mutex_p api_mutex; \
                        cond_p tag_cond_wait; \
                        tag_vtable_p vtable; \
                        tag_extended_callback_func callback; \
                        void *userdata; \
                        int64_t read_cache_expire; \
                        int64_t read_cache_ms; \
                        int64_t auto_sync_next_read; \
                        int64_t auto_sync_next_write



struct plc_tag_t {
    TAG_BASE_STRUCT;
};

#define PLC_TAG_P_NULL ((plc_tag_p)0)


/* the following may need to be used where the tag is already mapped or is not yet mapped */
int lib_init(void);
void lib_teardown(void);
void plc_tag_generic_tickler(plc_tag_p tag);
#define plc_tag_generic_raise_event(t, e, s) plc_tag_generic_raise_event_impl(__func__, __LINE__, t, e, s)
int plc_tag_generic_raise_event_impl(const char *func, int line_num, plc_tag_p tag, int8_t event_val, int8_t status);
void plc_tag_generic_handle_event_callbacks(plc_tag_p tag);
#define plc_tag_tickler_wake()  plc_tag_tickler_wake_impl(__func__, __LINE__)
int plc_tag_tickler_wake_impl(const char *func, int line_num);
#define plc_tag_generic_wake_tag(tag) plc_tag_generic_wake_tag_impl(__func__, __LINE__, tag)
int plc_tag_generic_wake_tag_impl(const char *func, int line_num, plc_tag_p tag);
int plc_tag_generic_init_tag(plc_tag_p tag, attr attributes, void (*tag_callback_func)(int32_t tag_id, int event, int status, void *userdata), void *userdata);

static inline void tag_raise_event(plc_tag_p tag, int event, int8_t status)
{
    /* do not stack up events if there is no callback. */
    if(!tag->callback) {
        return;
    }

    switch(event) {
        case PLCTAG_EVENT_ABORTED:
            pdebug(DEBUG_DETAIL, "PLCTAG_EVENT_ABORTED raised with status %s.", plc_tag_decode_error(status));
            tag->event_operation_aborted = 1;
            tag->event_operation_aborted_status = status;
            if(!tag->had_created_event) {
                pdebug(DEBUG_DETAIL, "Raising synthesized created event on abort event.");
                tag->had_created_event = 1;
                tag->event_creation_complete = 1;
                tag->event_creation_complete_status = status;
            }
            break;

        case PLCTAG_EVENT_CREATED:
            pdebug(DEBUG_DETAIL, "PLCTAG_EVENT_CREATED raised with status %s.", plc_tag_decode_error(status));
            if(!tag->had_created_event) {
                tag->event_creation_complete = 1;
                tag->event_creation_complete_status = status;
                tag->had_created_event = 1;
            } else {
                pdebug(DEBUG_DETAIL, "PLCTAG_EVENT_CREATED skipped due to duplication.");
            }
            break;

        case PLCTAG_EVENT_DESTROYED:
            pdebug(DEBUG_DETAIL, "PLCTAG_EVENT_DESTROYED raised with status %s.", plc_tag_decode_error(status));
            tag->event_deletion_started = 1;
            tag->event_deletion_started_status = status;
            break;

        case PLCTAG_EVENT_READ_COMPLETED:
            pdebug(DEBUG_DETAIL, "PLCTAG_EVENT_READ_COMPLETED raised with status %s.", plc_tag_decode_error(status));
            if(!tag->had_created_event) {
                pdebug(DEBUG_DETAIL, "Raising synthesized created event on read completed event.");
                tag->had_created_event = 1;
                tag->event_creation_complete = 1;
                tag->event_creation_complete_status = status;
            }

            if(tag->event_read_complete_enable) {
                tag->event_read_complete = 1;
                tag->event_read_complete_status = status;
                tag->event_read_complete_enable = 0;
                pdebug(DEBUG_DETAIL, "Disabled PLCTAG_EVENT_READ_COMPLETE.");
            }
            break;

        case PLCTAG_EVENT_READ_STARTED:
            pdebug(DEBUG_DETAIL, "PLCTAG_EVENT_READ_STARTED raised with status %s.", plc_tag_decode_error(status));
            tag->event_read_started = 1;
            tag->event_read_started_status = status;
            tag->event_read_complete_enable = 1;
            pdebug(DEBUG_DETAIL, "Enabled PLCTAG_EVENT_READ_COMPLETE.");
            break;

        case PLCTAG_EVENT_WRITE_COMPLETED:
            pdebug(DEBUG_DETAIL, "PLCTAG_EVENT_WRITE_COMPLETED raised with status %s.", plc_tag_decode_error(status));
            if(!tag->had_created_event) {
                pdebug(DEBUG_DETAIL, "Raising synthesized created event on write completed event.");
                tag->had_created_event = 1;
                tag->event_creation_complete = 1;
                tag->event_creation_complete_status = status;
            }

            if(tag->event_write_complete_enable) {
                tag->event_write_complete = 1;
                tag->event_write_complete_status = status;
                tag->event_write_complete_enable = 0;
                pdebug(DEBUG_DETAIL, "Disabled PLCTAG_EVENT_WRITE_COMPLETE.");
            }
            break;

        case PLCTAG_EVENT_WRITE_STARTED:
            pdebug(DEBUG_DETAIL, "PLCTAG_EVENT_WRITE_STARTED raised with status %s.", plc_tag_decode_error(status));
            tag->event_write_started = 1;
            tag->event_write_started_status = status;
            tag->event_write_complete_enable = 1;
            pdebug(DEBUG_DETAIL, "Enabled PLCTAG_EVENT_WRITE_COMPLETE.");
            break;

        default:
            pdebug(DEBUG_WARN, "Unsupported event %d!");
            break;
    }
}

#endif // __LIB_TAG_H__


#ifndef __LIB_INIT_H__
#define __LIB_INIT_H__

int initialize_modules(void);
typedef plc_tag_p(*tag_create_function)(attr attributes, void (*tag_callback_func)(int32_t tag_id, int event, int status, void* userdata), void* userdata);
tag_create_function find_tag_create_func(attr attributes);
void destroy_modules(void);

#endif // __LIB_INIT_H__


#ifndef __LIB_VERSION_H__
#define __LIB_VERSION_H__

/*
 * The library version in various ways.
 *
 * The defines are for building in specific versions and then
 * checking them against a dynamically linked library.
 */

/*

Version manually added - Adam Lafontaine

*/

#define VERSION "LIBPLCTAG 2.5.0"
#define version_major ((uint64_t)2)
#define version_minor ((uint64_t)5)
#define version_patch ((uint64_t)0)

#endif // __LIB_VERSION_H__


#ifndef __PROTOCOLS_MB_MODBUS_H__
#define __PROTOCOLS_MB_MODBUS_H__

void mb_teardown(void);
int mb_init();
plc_tag_p mb_tag_create(attr attribs, void (*tag_callback_func)(int32_t tag_id, int event, int status, void *userdata), void *userdata);

#endif // __PROTOCOLS_MB_MODBUS_H__


#ifndef __PROTOCOLS_SYSTEM_SYSTEM_H__
#define __PROTOCOLS_SYSTEM_SYSTEM_H__

plc_tag_p system_tag_create(attr attribs, void (*tag_callback_func)(int32_t tag_id, int event, int status, void *userdata), void *userdata);

#endif // __PROTOCOLS_SYSTEM_SYSTEM_H__


#ifndef __PROTOCOLS_SYSTEM_TAG_H__
#define __PROTOCOLS_SYSTEM_TAG_H__

#define MAX_SYSTEM_TAG_NAME (20)
#define MAX_SYSTEM_TAG_SIZE (30)

struct system_tag_t {
    /*struct plc_tag_t p_tag;*/
    TAG_BASE_STRUCT;

    char name[MAX_SYSTEM_TAG_NAME];
    uint8_t backing_data[MAX_SYSTEM_TAG_SIZE];
};

typedef struct system_tag_t *system_tag_p;

#endif // __PROTOCOLS_SYSTEM_TAG_H__


#ifndef __PROTOCOLS_AB_DEFS_H__
#define __PROTOCOLS_AB_DEFS_H__

#define AB_EIP_PLC5_PARAM ((uint16_t)0x4302)
#define AB_EIP_SLC_PARAM ((uint16_t)0x4302)
#define AB_EIP_LGX_PARAM ((uint16_t)0x43F8)


#define AB_EIP_CONN_PARAM ((uint16_t)0x4200)
//0100 0011 1111 1000
//0100 001 1 1111 1000
#define AB_EIP_CONN_PARAM_EX ((uint32_t)0x42000000)
//0100 001 0 0000 0000  0000 0100 0000 0000
//0x42000400


#define DEFAULT_MAX_REQUESTS (10)   /* number of requests and request sizes to allocate by default. */


/* AB Constants*/
#define AB_EIP_OK   (0)
#define AB_EIP_VERSION ((uint16_t)0x0001)

/* in milliseconds */
#define AB_EIP_DEFAULT_TIMEOUT 2000 /* in ms */

/* AB Commands */
#define AB_EIP_REGISTER_SESSION     ((uint16_t)0x0065)
#define AB_EIP_UNREGISTER_SESSION   ((uint16_t)0x0066)
#define AB_EIP_UNCONNECTED_SEND     ((uint16_t)0x006F)
#define AB_EIP_CONNECTED_SEND       ((uint16_t)0x0070)

/* AB packet info */
#define AB_EIP_DEFAULT_PORT 44818

/* specific sub-commands */
#define AB_EIP_CMD_PCCC_EXECUTE         ((uint8_t)0x4B)
#define AB_EIP_CMD_FORWARD_CLOSE        ((uint8_t)0x4E)
#define AB_EIP_CMD_UNCONNECTED_SEND     ((uint8_t)0x52)
#define AB_EIP_CMD_FORWARD_OPEN         ((uint8_t)0x54)
#define AB_EIP_CMD_FORWARD_OPEN_EX      ((uint8_t)0x5B)

/* CIP embedded packet commands */
#define AB_EIP_CMD_CIP_GET_ATTR_LIST    ((uint8_t)0x03)
#define AB_EIP_CMD_CIP_MULTI            ((uint8_t)0x0A)
#define AB_EIP_CMD_CIP_READ             ((uint8_t)0x4C)
#define AB_EIP_CMD_CIP_WRITE            ((uint8_t)0x4D)
#define AB_EIP_CMD_CIP_RMW              ((uint8_t)0x4E)
#define AB_EIP_CMD_CIP_READ_FRAG        ((uint8_t)0x52)
#define AB_EIP_CMD_CIP_WRITE_FRAG       ((uint8_t)0x53)
#define AB_EIP_CMD_CIP_LIST_TAGS        ((uint8_t)0x55)

/* flag set when command is OK */
#define AB_EIP_CMD_CIP_OK               ((uint8_t)0x80)

#define AB_CIP_STATUS_OK                ((uint8_t)0x00)
#define AB_CIP_STATUS_FRAG              ((uint8_t)0x06)

#define AB_CIP_ERR_UNSUPPORTED_SERVICE  ((uint8_t)0x08)
#define AB_CIP_ERR_PARTIAL_ERROR  ((uint8_t)0x1e)

/* PCCC commands */
#define AB_EIP_PCCC_TYPED_CMD ((uint8_t)0x0F)
#define AB_EIP_PLC5_RANGE_READ_FUNC ((uint8_t)0x01)
#define AB_EIP_PLC5_RANGE_WRITE_FUNC ((uint8_t)0x00)
#define AB_EIP_PLC5_RMW_FUNC ((uint8_t)0x26)
#define AB_EIP_PCCCLGX_TYPED_READ_FUNC ((uint8_t)0x68)
#define AB_EIP_PCCCLGX_TYPED_WRITE_FUNC ((uint8_t)0x67)
#define AB_EIP_SLC_RANGE_READ_FUNC ((uint8_t)0xA2)
#define AB_EIP_SLC_RANGE_WRITE_FUNC ((uint8_t)0xAA)
#define AB_EIP_SLC_RANGE_BIT_WRITE_FUNC ((uint8_t)0xAB)



#define AB_PCCC_DATA_BIT            1
#define AB_PCCC_DATA_BIT_STRING     2
#define AB_PCCC_DATA_BYTE_STRING    3
#define AB_PCCC_DATA_INT            4
#define AB_PCCC_DATA_TIMER          5
#define AB_PCCC_DATA_COUNTER        6
#define AB_PCCC_DATA_CONTROL        7
#define AB_PCCC_DATA_REAL           8
#define AB_PCCC_DATA_ARRAY          9
#define AB_PCCC_DATA_ADDRESS        15
#define AB_PCCC_DATA_BCD            16




/* base data type byte values */
#define AB_CIP_DATA_BIT         ((uint8_t)0xC1) /* Boolean value, 1 bit */
#define AB_CIP_DATA_SINT        ((uint8_t)0xC2) /* Signed 8–bit integer value */
#define AB_CIP_DATA_INT         ((uint8_t)0xC3) /* Signed 16–bit integer value */
#define AB_CIP_DATA_DINT        ((uint8_t)0xC4) /* Signed 32–bit integer value */
#define AB_CIP_DATA_LINT        ((uint8_t)0xC5) /* Signed 64–bit integer value */
#define AB_CIP_DATA_USINT       ((uint8_t)0xC6) /* Unsigned 8–bit integer value */
#define AB_CIP_DATA_UINT        ((uint8_t)0xC7) /* Unsigned 16–bit integer value */
#define AB_CIP_DATA_UDINT       ((uint8_t)0xC8) /* Unsigned 32–bit integer value */
#define AB_CIP_DATA_ULINT       ((uint8_t)0xC9) /* Unsigned 64–bit integer value */
#define AB_CIP_DATA_REAL        ((uint8_t)0xCA) /* 32–bit floating point value, IEEE format */
#define AB_CIP_DATA_LREAL       ((uint8_t)0xCB) /* 64–bit floating point value, IEEE format */
#define AB_CIP_DATA_STIME       ((uint8_t)0xCC) /* Synchronous time value */
#define AB_CIP_DATA_DATE        ((uint8_t)0xCD) /* Date value */
#define AB_CIP_DATA_TIME_OF_DAY ((uint8_t)0xCE) /* Time of day value */
#define AB_CIP_DATA_DATE_AND_TIME ((uint8_t)0xCF) /* Date and time of day value */
#define AB_CIP_DATA_STRING      ((uint8_t)0xD0) /* Character string, 1 byte per character */
#define AB_CIP_DATA_BYTE        ((uint8_t)0xD1) /* 8-bit bit string */
#define AB_CIP_DATA_WORD        ((uint8_t)0xD2) /* 16-bit bit string */
#define AB_CIP_DATA_DWORD       ((uint8_t)0xD3) /* 32-bit bit string */
#define AB_CIP_DATA_LWORD       ((uint8_t)0xD4) /* 64-bit bit string */
#define AB_CIP_DATA_STRING2     ((uint8_t)0xD5) /* Wide char character string, 2 bytes per character */
#define AB_CIP_DATA_FTIME       ((uint8_t)0xD6) /* High resolution duration value */
#define AB_CIP_DATA_LTIME       ((uint8_t)0xD7) /* Medium resolution duration value */
#define AB_CIP_DATA_ITIME       ((uint8_t)0xD8) /* Low resolution duration value */
#define AB_CIP_DATA_STRINGN     ((uint8_t)0xD9) /* N-byte per char character string */
#define AB_CIP_DATA_SHORT_STRING ((uint8_t)0xDA) /* Counted character sting with 1 byte per character and 1 byte length indicator */
#define AB_CIP_DATA_TIME        ((uint8_t)0xDB) /* Duration in milliseconds */
#define AB_CIP_DATA_EPATH       ((uint8_t)0xDC) /* CIP path segment(s) */
#define AB_CIP_DATA_ENGUNIT     ((uint8_t)0xDD) /* Engineering units */
#define AB_CIP_DATA_STRINGI     ((uint8_t)0xDE) /* International character string (encoding?) */

/* aggregate data type byte values */
#define AB_CIP_DATA_ABREV_STRUCT    ((uint8_t)0xA0) /* Data is an abbreviated struct type, i.e. a CRC of the actual type descriptor */
#define AB_CIP_DATA_ABREV_ARRAY     ((uint8_t)0xA1) /* Data is an abbreviated array type. The limits are left off */
#define AB_CIP_DATA_FULL_STRUCT     ((uint8_t)0xA2) /* Data is a struct type descriptor */
#define AB_CIP_DATA_FULL_ARRAY      ((uint8_t)0xA3) /* Data is an array type descriptor */


/* transport class */
#define AB_EIP_TRANSPORT_CLASS_T3   ((uint8_t)0xA3)


#define AB_EIP_SECS_PER_TICK 0x0A
#define AB_EIP_TIMEOUT_TICKS 0x05
#define AB_EIP_VENDOR_ID 0xF33D /*tres 1337 */
#define AB_EIP_VENDOR_SN 0x21504345  /* the string !PCE */
#define AB_EIP_TIMEOUT_MULTIPLIER 0x01
#define AB_EIP_RPI  1000000

//#define AB_EIP_TRANSPORT 0xA3


/* EIP Item Types */
#define AB_EIP_ITEM_NAI ((uint16_t)0x0000) /* NULL Address Item */
#define AB_EIP_ITEM_CAI ((uint16_t)0x00A1) /* connected address item */
#define AB_EIP_ITEM_CDI ((uint16_t)0x00B1) /* connected data item */
#define AB_EIP_ITEM_UDI ((uint16_t)0x00B2) /* Unconnected data item */


/* Types of AB protocols */
//#define AB_PLC_PLC         (1)
//#define AB_PLC_MLGX        (2)
//#define AB_PLC_LGX         (3)
//#define AB_PLC_MICRO800     (4)
//#define AB_PLC_LGX_PCCC    (5)

typedef enum { AB_PLC_NONE = 0, AB_PLC_PLC5 = 1, AB_PLC_SLC, AB_PLC_MLGX, AB_PLC_LGX, AB_PLC_LGX_PCCC, AB_PLC_MICRO800, AB_PLC_OMRON_NJNX } plc_type_t;


/*********************************************************************
 ************************ AB EIP Structures **************************
 ********************************************************************/


START_PACK typedef struct {
    uint8_t reply_service;          /* 0x?? CIP reply */
    uint8_t reserved;               /* 0x00 in reply */
    uint8_t status;                 /* 0x00 for success */
    uint8_t num_status_words;       /* number of 16-bit words in status */
} END_PACK cip_header;

START_PACK typedef struct {
    uint8_t service_code;        /* ALWAYS 0x0A Forward Open Request */
    uint8_t req_path_size;       /* ALWAYS 2, size in words of path, next field */
    uint8_t req_path[4];         /* ALWAYS 0x20,0x06,0x24,0x01 for CM, instance 1*/
    uint16_le request_count;        /* number of requests packed in this packet. */
    uint16_le request_offsets[];    /* request offsets from the count */
} END_PACK cip_multi_req_header;

START_PACK typedef struct {
    uint8_t reply_service;          /* 0x?? CIP reply */
    uint8_t reserved;               /* 0x00 in reply */
    uint8_t status;                 /* 0x00 for success */
    uint8_t num_status_words;       /* number of 16-bit words in status */
    uint16_le request_count;        /* number of requests packed in this packet. */
    uint16_le request_offsets[];    /* request offsets from the count */
} END_PACK cip_multi_resp_header;

/* EIP Encapsulation Header */
START_PACK typedef struct {
    uint16_le encap_command;
    uint16_le encap_length;
    uint32_le encap_session_handle;
    uint32_le encap_status;
    uint64_le encap_sender_context;
    uint32_le encap_options;
} END_PACK eip_encap;

/* Session Registration Request */
START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;         /* ALWAYS 0x0065 Register Session*/
    uint16_le encap_length;   /* packet size in bytes - 24 */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;  /* whatever we want to set this to, used for
                                     * identifying responses when more than one
                                     * are in flight at once.
                                     */
    uint32_le encap_options;         /* 0, reserved for future use */

    /* session registration request */
    uint16_le eip_version;
    uint16_le option_flags;
} END_PACK eip_session_reg_req;


/* Forward Open Request */
START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;    /* ALWAYS 0x006f Unconnected Send*/
    uint16_le encap_length;   /* packet size in bytes - 24 */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;  /* whatever we want to set this to, used for
                                     * identifying responses when more than one
                                     * are in flight at once.
                                     */
    uint32_le encap_options;         /* 0, reserved for future use */

    /* Interface Handle etc. */
    uint32_le interface_handle;      /* ALWAYS 0 */
    uint16_le router_timeout;        /* in seconds */

    /* Common Packet Format - CPF Unconnected */
    uint16_le cpf_item_count;        /* ALWAYS 2 */
    uint16_le cpf_nai_item_type;     /* ALWAYS 0 */
    uint16_le cpf_nai_item_length;   /* ALWAYS 0 */
    uint16_le cpf_udi_item_type;     /* ALWAYS 0x00B2 - Unconnected Data Item */
    uint16_le cpf_udi_item_length;   /* REQ: fill in with length of remaining data. */

    /* CM Service Request - Connection Manager */
    uint8_t cm_service_code;        /* ALWAYS 0x54 Forward Open Request */
    uint8_t cm_req_path_size;       /* ALWAYS 2, size in words of path, next field */
    uint8_t cm_req_path[4];         /* ALWAYS 0x20,0x06,0x24,0x01 for CM, instance 1*/

    /* Forward Open Params */
    uint8_t secs_per_tick;          /* seconds per tick */
    uint8_t timeout_ticks;          /* timeout = srd_secs_per_tick * src_timeout_ticks */
    uint32_le orig_to_targ_conn_id;  /* 0, returned by target in reply. */
    uint32_le targ_to_orig_conn_id;  /* what is _our_ ID for this connection, use ab_connection ptr as id ? */
    uint16_le conn_serial_number;    /* our connection ID/serial number */
    uint16_le orig_vendor_id;        /* our unique vendor ID */
    uint32_le orig_serial_number;    /* our unique serial number */
    uint8_t conn_timeout_multiplier;/* timeout = mult * RPI */
    uint8_t reserved[3];            /* reserved, set to 0 */
    uint32_le orig_to_targ_rpi;      /* us to target RPI - Request Packet Interval in microseconds */
    uint16_le orig_to_targ_conn_params; /* some sort of identifier of what kind of PLC we are??? */
    uint32_le targ_to_orig_rpi;      /* target to us RPI, in microseconds */
    uint16_le targ_to_orig_conn_params; /* some sort of identifier of what kind of PLC the target is ??? */
    uint8_t transport_class;        /* ALWAYS 0xA3, server transport, class 3, application trigger */
    uint8_t path_size;              /* size of connection path in 16-bit words
                                     * connection path from MSG instruction.
                                     *
                                     * EG LGX with 1756-ENBT and CPU in slot 0 would be:
                                     * 0x01 - backplane port of 1756-ENBT
                                     * 0x00 - slot 0 for CPU
                                     * 0x20 - class
                                     * 0x02 - MR Message Router
                                     * 0x24 - instance
                                     * 0x01 - instance #1.
                                     */

    //uint8_t conn_path[ZLA_SIZE];    /* connection path as above */
} END_PACK eip_forward_open_request_t;


/* Forward Open Request Extended */
START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;    /* ALWAYS 0x006f Unconnected Send*/
    uint16_le encap_length;   /* packet size in bytes - 24 */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;  /* whatever we want to set this to, used for
                                     * identifying responses when more than one
                                     * are in flight at once.
                                     */
    uint32_le encap_options;         /* 0, reserved for future use */

    /* Interface Handle etc. */
    uint32_le interface_handle;      /* ALWAYS 0 */
    uint16_le router_timeout;        /* in seconds */

    /* Common Packet Format - CPF Unconnected */
    uint16_le cpf_item_count;        /* ALWAYS 2 */
    uint16_le cpf_nai_item_type;     /* ALWAYS 0 */
    uint16_le cpf_nai_item_length;   /* ALWAYS 0 */
    uint16_le cpf_udi_item_type;     /* ALWAYS 0x00B2 - Unconnected Data Item */
    uint16_le cpf_udi_item_length;   /* REQ: fill in with length of remaining data. */

    /* CM Service Request - Connection Manager */
    uint8_t cm_service_code;        /* ALWAYS 0x54 Forward Open Request */
    uint8_t cm_req_path_size;       /* ALWAYS 2, size in words of path, next field */
    uint8_t cm_req_path[4];         /* ALWAYS 0x20,0x06,0x24,0x01 for CM, instance 1*/

    /* Forward Open Params */
    uint8_t secs_per_tick;          /* seconds per tick */
    uint8_t timeout_ticks;          /* timeout = srd_secs_per_tick * src_timeout_ticks */
    uint32_le orig_to_targ_conn_id;  /* 0, returned by target in reply. */
    uint32_le targ_to_orig_conn_id;  /* what is _our_ ID for this connection, use ab_connection ptr as id ? */
    uint16_le conn_serial_number;    /* our connection ID/serial number ?? */
    uint16_le orig_vendor_id;        /* our unique vendor ID */
    uint32_le orig_serial_number;    /* our unique serial number */
    uint8_t conn_timeout_multiplier;/* timeout = mult * RPI */
    uint8_t reserved[3];            /* reserved, set to 0 */
    uint32_le orig_to_targ_rpi;      /* us to target RPI - Request Packet Interval in microseconds */
    uint32_le orig_to_targ_conn_params_ex; /* some sort of identifier of what kind of PLC we are??? */
    uint32_le targ_to_orig_rpi;      /* target to us RPI, in microseconds */
    uint32_le targ_to_orig_conn_params_ex; /* some sort of identifier of what kind of PLC the target is ??? */
    uint8_t transport_class;        /* ALWAYS 0xA3, server transport, class 3, application trigger */
    uint8_t path_size;              /* size of connection path in 16-bit words
                                     * connection path from MSG instruction.
                                     *
                                     * EG LGX with 1756-ENBT and CPU in slot 0 would be:
                                     * 0x01 - backplane port of 1756-ENBT
                                     * 0x00 - slot 0 for CPU
                                     * 0x20 - class
                                     * 0x02 - MR Message Router
                                     * 0x24 - instance
                                     * 0x01 - instance #1.
                                     */

    //uint8_t conn_path[ZLA_SIZE];    /* connection path as above */
} END_PACK eip_forward_open_request_ex_t;




/* Forward Open Response */
START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;    /* ALWAYS 0x006f Unconnected Send*/
    uint16_le encap_length;   /* packet size in bytes - 24 */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;/* whatever we want to set this to, used for
                                     * identifying responses when more than one
                                     * are in flight at once.
                                     */
    uint32_le options;               /* 0, reserved for future use */

    /* Interface Handle etc. */
    uint32_le interface_handle;      /* ALWAYS 0 */
    uint16_le router_timeout;        /* in seconds */

    /* Common Packet Format - CPF Unconnected */
    uint16_le cpf_item_count;        /* ALWAYS 2 */
    uint16_le cpf_nai_item_type;     /* ALWAYS 0 */
    uint16_le cpf_nai_item_length;   /* ALWAYS 0 */
    uint16_le cpf_udi_item_type;     /* ALWAYS 0x00B2 - Unconnected Data Item */
    uint16_le cpf_udi_item_length;   /* REQ: fill in with length of remaining data. */

    /* Forward Open Reply */
    uint8_t resp_service_code;      /* returned as 0xD4 or 0xDB */
    uint8_t reserved1;               /* returned as 0x00? */
    uint8_t general_status;         /* 0 on success */
    uint8_t status_size;            /* number of 16-bit words of extra status, 0 if success */
    uint32_le orig_to_targ_conn_id;  /* target's connection ID for us, save this. */
    uint32_le targ_to_orig_conn_id;  /* our connection ID back for reference */
    uint16_le conn_serial_number;    /* our connection ID/serial number from request */
    uint16_le orig_vendor_id;        /* our unique vendor ID from request*/
    uint32_le orig_serial_number;    /* our unique serial number from request*/
    uint32_le orig_to_targ_api;      /* Actual packet interval, microsecs */
    uint32_le targ_to_orig_api;      /* Actual packet interval, microsecs */
    uint8_t app_data_size;          /* size in 16-bit words of send_data at end */
    uint8_t reserved2;
    //uint8_t app_data[ZLA_SIZE];
} END_PACK eip_forward_open_response_t;



/* Forward Close Request */
START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;    /* ALWAYS 0x006f Unconnected Send*/
    uint16_le encap_length;   /* packet size in bytes - 24 */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;/* whatever we want to set this to, used for
                                     * identifying responses when more than one
                                     * are in flight at once.
                                     */
    uint32_le encap_options;         /* 0, reserved for future use */

    /* Interface Handle etc. */
    uint32_le interface_handle;      /* ALWAYS 0 */
    uint16_le router_timeout;        /* in seconds */

    /* Common Packet Format - CPF Unconnected */
    uint16_le cpf_item_count;        /* ALWAYS 2 */
    uint16_le cpf_nai_item_type;     /* ALWAYS 0 */
    uint16_le cpf_nai_item_length;   /* ALWAYS 0 */
    uint16_le cpf_udi_item_type;     /* ALWAYS 0x00B2 - Unconnected Data Item */
    uint16_le cpf_udi_item_length;   /* REQ: fill in with length of remaining data. */

    /* CM Service Request - Connection Manager */
    uint8_t cm_service_code;        /* ALWAYS 0x4E Forward Close Request */
    uint8_t cm_req_path_size;       /* ALWAYS 2, size in words of path, next field */
    uint8_t cm_req_path[4];         /* ALWAYS 0x20,0x06,0x24,0x01 for CM, instance 1*/

    /* Forward Open Params */
    uint8_t secs_per_tick;       /* seconds per tick */
    uint8_t timeout_ticks;       /* timeout = srd_secs_per_tick * src_timeout_ticks */
    uint16_le conn_serial_number;    /* our connection ID/serial number */
    uint16_le orig_vendor_id;        /* our unique vendor ID */
    uint32_le orig_serial_number;    /* our unique serial number */
    uint8_t path_size;              /* size of connection path in 16-bit words*/
    uint8_t reserved;               /* ALWAYS 0 */
    //uint8_t conn_path[ZLA_SIZE];
} END_PACK eip_forward_close_req_t;



/* Forward Close Response */
START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;    /* ALWAYS 0x006f Unconnected Send*/
    uint16_le encap_length;   /* packet size in bytes - 24 */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;/* whatever we want to set this to, used for
                                     * identifying responses when more than one
                                     * are in flight at once.
                                     */
    uint32_le encap_options;         /* 0, reserved for future use */

    /* Interface Handle etc. */
    uint32_le interface_handle;      /* ALWAYS 0 */
    uint16_le router_timeout;        /* in seconds */

    /* Common Packet Format - CPF Unconnected */
    uint16_le cpf_item_count;        /* ALWAYS 2 */
    uint16_le cpf_nai_item_type;     /* ALWAYS 0 */
    uint16_le cpf_nai_item_length;   /* ALWAYS 0 */
    uint16_le cpf_udi_item_type;     /* ALWAYS 0x00B2 - Unconnected Data Item */
    uint16_le cpf_udi_item_length;   /* REQ: fill in with length of remaining data. */

    /* Forward Close Response */
    uint8_t resp_service_code;      /* returned as 0xCE */
    uint8_t reserved1;               /* returned as 0x00? */
    uint8_t general_status;         /* 0 on success */
    uint8_t status_size;            /* number of 16-bit words of extra status, 0 if success */
    uint16_le conn_serial_number;    /* our connection ID/serial number ?? */
    uint16_le orig_vendor_id;        /* our unique vendor ID */
    uint32_le orig_serial_number;    /* our unique serial number */
    uint8_t path_size;              /* size of connection path in 16-bit words*/
    uint8_t reserved;               /* ALWAYS 0 */
    //uint8_t conn_path[ZLA_SIZE];
} END_PACK eip_forward_close_resp_t;


/* CIP generic connected response */
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
} END_PACK eip_cip_co_generic_response;



/* PCCC Request */
START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;    /* ALWAYS 0x0070 Connected Send */
    uint16_le encap_length;   /* packet size in bytes less the header size, which is 24 bytes */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;/* whatever we want to set this to, used for
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
    //uint8_t pccc_data[ZLA_SIZE];   /* send_data for request */
} END_PACK eip_pccc_req_old;




/* PCCC Response */
START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;    /* ALWAYS 0x0070 Connected Send */
    uint16_le encap_length;   /* packet size in bytes less the header size, which is 24 bytes */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;/* whatever we want to set this to, used for
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

    /* PCCC Reply */
    uint8_t reply_service;          /* 0xCB Execute PCCC Reply */
    uint8_t reserved;               /* 0x00 in reply */
    uint8_t general_status;         /* 0x00 for success */
    uint8_t status_size;            /* number of 16-bit words of extra status, 0 if success */

    /* PCCC Command Req Routing */
    uint8_t request_id_size;        /* ALWAYS 7 */
    uint16_le vendor_id;             /* Our CIP Vendor ID */
    uint32_le vendor_serial_number;  /* Our CIP Vendor Serial Number */

    /* PCCC Command */
    uint8_t pccc_command;           /* CMD read, write etc. */
    uint8_t pccc_status;            /* STS 0x00 in request */
    uint16_le pccc_seq_num;          /* TNSW transaction/connection sequence number */
    //uint8_t pccc_data[ZLA_SIZE];    /* data for PCCC request. */
} END_PACK eip_pccc_resp_old;



/* PCCC Request PLC5 DH+ Only */
//START_PACK typedef struct {
//    /* encap header */
//    uint16_le encap_command;    /* ALWAYS 0x0070 Connected Send */
//    uint16_le encap_length;   /* packet size in bytes less the header size, which is 24 bytes */
//    uint32_le encap_session_handle;  /* from session set up */
//    uint32_le encap_status;          /* always _sent_ as 0 */
//    uint64_le encap_sender_context;  /* whatever we want to set this to, used for
//                                     * identifying responses when more than one
//                                     * are in flight at once.
//                                     */
//    uint32_le options;               /* 0, reserved for future use */
//
//    /* Interface Handle etc. */
//    uint32_le interface_handle;      /* ALWAYS 0 */
//    uint16_le router_timeout;        /* in seconds, zero for Connected Sends! */
//
//    /* Common Packet Format - CPF Connected */
//    uint16_le cpf_item_count;        /* ALWAYS 2 */
//    uint16_le cpf_cai_item_type;     /* ALWAYS 0x00A1 Connected Address Item */
//    uint16_le cpf_cai_item_length;   /* ALWAYS 2 ? */
//    uint32_le cpf_targ_conn_id;           /* the connection id from Forward Open */
//    uint16_le cpf_cdi_item_type;     /* ALWAYS 0x00B1, Connected Data Item type */
//    uint16_le cpf_cdi_item_length;   /* length in bytes of the rest of the packet */
//
//    /* Connection sequence number */
//    uint16_le cpf_conn_seq_num;      /* connection sequence ID, inc for each message */
//
//    /* PLC5 DH+ Routing */
//    uint16_le dest_link;
//    uint16_le dest_node;
//    uint16_le src_link;
//    uint16_le src_node;
//
//    /* PCCC Command */
//    uint8_t pccc_command;           /* CMD read, write etc. */
//    uint8_t pccc_status;            /* STS 0x00 in request */
//    uint16_le pccc_seq_num;          /* TNSW transaction/sequence id */
//    uint8_t pccc_function;          /* FNC sub-function of command */
//    uint16_le pccc_transfer_offset;           /* offset of this request? */
//    uint16_le pccc_transfer_size;    /* number of elements requested */
//    //uint8_t pccc_data[ZLA_SIZE];    /* send_data for request */
//} END_PACK pccc_dhp_co_req;
//



/* PCCC PLC5 DH+ Only Response */
//START_PACK typedef struct {
//    /* encap header */
//    uint16_le encap_command;    /* ALWAYS 0x0070 Connected Send */
//    uint16_le encap_length;   /* packet size in bytes less the header size, which is 24 bytes */
//    uint32_le encap_session_handle;  /* from session set up */
//    uint32_le encap_status;          /* always _sent_ as 0 */
//    uint64_le encap_sender_context;  /* whatever we want to set this to, used for
//                                     * identifying responses when more than one
//                                     * are in flight at once.
//                                     */
//    uint32_le options;               /* 0, reserved for future use */
//
//    /* Interface Handle etc. */
//    uint32_le interface_handle;      /* ALWAYS 0 */
//    uint16_le router_timeout;        /* in seconds, zero for Connected Sends! */
//
//    /* Common Packet Format - CPF Connected */
//    uint16_le cpf_item_count;        /* ALWAYS 2 */
//    uint16_le cpf_cai_item_type;     /* ALWAYS 0x00A1 Connected Address Item */
//    uint16_le cpf_cai_item_length;   /* ALWAYS 2 ? */
//    uint32_le cpf_targ_conn_id;           /* the connection id from Forward Open */
//    uint16_le cpf_cdi_item_type;     /* ALWAYS 0x00B1, Connected Data Item type */
//    uint16_le cpf_cdi_item_length;   /* length in bytes of the rest of the packet */
//
//    /* connection ID from request */
//    uint16_le cpf_conn_seq_num;      /* connection sequence ID, inc for each message */
//
//    /* PLC5 DH+ Routing */
//    uint16_le dest_link;
//    uint16_le dest_node;
//    uint16_le src_link;
//    uint16_le src_node;
//
//    /* PCCC Command */
//    uint8_t pccc_command;           /* CMD read, write etc. */
//    uint8_t pccc_status;            /* STS 0x00 in request */
//    uint16_le pccc_seq_num;         /* TNSW transaction/connection sequence number */
//    //uint8_t pccc_data[ZLA_SIZE];    /* data for PCCC request. */
//} END_PACK pccc_dhp_co_resp;



/* CIP "native" Request */
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

    /* CIP Service Info */
    //uint8_t service_code;           /* ALWAYS 0x4C, CIP_READ */
    /*uint8_t req_path_size;*/          /* path size in words */
    //uint8_t req_path[ZLA_SIZE];
} END_PACK eip_cip_co_req;




/* CIP Response */
START_PACK typedef struct {
    /* encap header */
    uint16_le encap_command;    /* ALWAYS 0x0070 Connected Send */
    uint16_le encap_length;   /* packet size in bytes less the header size, which is 24 bytes */
    uint32_le encap_session_handle;  /* from session set up */
    uint32_le encap_status;          /* always _sent_ as 0 */
    uint64_le encap_sender_context;/* whatever we want to set this to, used for
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
    uint32_le cpf_orig_conn_id;      /* our connection ID, NOT the target's */
    uint16_le cpf_cdi_item_type;     /* ALWAYS 0x00B1, Connected Data Item type */
    uint16_le cpf_cdi_item_length;   /* length in bytes of the rest of the packet */

    /* connection ID from request */
    uint16_le cpf_conn_seq_num;      /* connection sequence ID, inc for each message */

    /* CIP Reply */
    uint8_t reply_service;          /* 0xCC CIP READ Reply */
    uint8_t reserved;               /* 0x00 in reply */
    uint8_t status;                 /* 0x00 for success */
    uint8_t num_status_words;       /* number of 16-bit words in status */

    /* CIP Data*/
    //uint8_t resp_data[ZLA_SIZE];
} END_PACK eip_cip_co_resp;



/* CIP "native" Unconnected Request */
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

    /* CM Service Request - Connection Manager */
    /* NOTE, we overlay the following if this is PCCC */
    uint8_t cm_service_code;        /* ALWAYS 0x52 Unconnected Send */
    uint8_t cm_req_path_size;       /* ALWAYS 2, size in words of path, next field */
    uint8_t cm_req_path[4];         /* ALWAYS 0x20,0x06,0x24,0x01 for CM, instance 1*/

    /* Unconnected send */
    uint8_t secs_per_tick;          /* seconds per tick */
    uint8_t timeout_ticks;          /* timeout = src_secs_per_tick * src_timeout_ticks */

    /* size ? */
    uint16_le uc_cmd_length;         /* length of embedded packet */

    /* CIP read/write request, embedded packet */

    /* IOI path to target device, connection IOI */
} END_PACK eip_cip_uc_req;




/* CIP "native" Unconnected Response */
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

    /* CIP read/write response, embedded packet */
    uint8_t reply_service;          /*  */
    uint8_t reserved;               /* 0x00 in reply */
    uint8_t status;                 /* 0x00 for success */
    uint8_t num_status_words;       /* number of 16-bit words in status */

} END_PACK eip_cip_uc_resp;


//
//START_PACK typedef struct {
//    /* encap header */
//    uint16_le encap_command;         /* ALWAYS 0x006f Unconnected Send*/
//    uint16_le encap_length;          /* packet size in bytes - 24 */
//    uint32_le encap_session_handle;  /* from session set up */
//    uint32_le encap_status;          /* always _sent_ as 0 */
//    uint64_le encap_sender_context;  /* whatever we want to set this to, used for
//                                     * identifying responses when more than one
//                                     * are in flight at once.
//                                     */
//    uint32_le encap_options;         /* 0, reserved for future use */
//
//    /* Interface Handle etc. */
//    uint32_le interface_handle;      /* ALWAYS 0 */
//    uint16_le router_timeout;        /* in seconds, 5 or 10 seems to be good.*/
//
//    /* Common Packet Format - CPF Unconnected */
//    uint16_le cpf_item_count;        /* ALWAYS 2 */
//    uint16_le cpf_nai_item_type;     /* ALWAYS 0 */
//    uint16_le cpf_nai_item_length;   /* ALWAYS 0 */
//    uint16_le cpf_udi_item_type;     /* ALWAYS 0x00B2 - Unconnected Data Item */
//    uint16_le cpf_udi_item_length;   /* REQ: fill in with length of remaining data. */
//
//    /* PCCC Command Req Routing */
//    uint8_t service_code;           /* ALWAYS 0x4B, Execute PCCC */
//    uint8_t req_path_size;          /* ALWAYS 0x02, in 16-bit words */
//    uint8_t req_path[4];            /* ALWAYS 0x20,0x67,0x24,0x01 for PCCC */
//    uint8_t request_id_size;        /* ALWAYS 7 */
//    uint16_le vendor_id;             /* Our CIP Vendor ID */
//    uint32_le vendor_serial_number;  /* Our CIP Vendor Serial Number */
//
//    /* PCCC Command */
//    uint8_t pccc_command;           /* CMD read, write etc. */
//    uint8_t pccc_status;            /* STS 0x00 in request */
//    uint16_le pccc_seq_num;          /* TNS transaction/sequence id */
//    uint8_t pccc_function;          /* FNC sub-function of command */
////    uint16_le pccc_offset;           /* offset of requested in total request */
//    uint8_t pccc_transfer_size;    /* total number of bytes requested */
//    //uint8_t pccc_data[ZLA_SIZE];   /* send_data for request */
//} END_PACK pccc_req;
//


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

    /* PCCC Reply */
    uint8_t reply_code;          /* 0xCB Execute PCCC Reply */
    uint8_t reserved;               /* 0x00 in reply */
    uint8_t general_status;         /* 0x00 for success */
    uint8_t status_size;            /* number of 16-bit words of extra status, 0 if success */

    /* PCCC Command Req Routing */
    uint8_t request_id_size;        /* ALWAYS 7 */
    uint16_le vendor_id;             /* Our CIP Vendor ID */
    uint32_le vendor_serial_number;  /* Our CIP Vendor Serial Number */

    /* PCCC Command */
    uint8_t pccc_command;           /* CMD read, write etc. */
    uint8_t pccc_status;            /* STS 0x00 in request */
    uint16_le pccc_seq_num;          /* TNSW transaction/connection sequence number */
    //uint8_t pccc_data[ZLA_SIZE];    /* data for PCCC response. */
} END_PACK pccc_resp;





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

    /* CM Service Request - Connection Manager */
    /* NOTE, we overlay the following if this is PCCC */
    uint8_t cm_service_code;        /* ALWAYS 0x52 Unconnected Send */
    uint8_t cm_req_path_size;       /* ALWAYS 2, size in words of path, next field */
    uint8_t cm_req_path[6];         /* ALWAYS 0x20,0x06,0x24,0x01 for CM, instance 1*/

    /* Unconnected send */
    uint8_t secs_per_tick;          /* seconds per tick */
    uint8_t timeout_ticks;          /* timeout = src_secs_per_tick * src_timeout_ticks */

    /* size ? */
    uint16_le uc_cmd_length;         /* length of embedded packet */

    /* needed when talking to PLC5 over DH+ */
    uint16_le dest_link;
    uint16_le dest_node;
    uint16_le src_link;
    uint16_le src_node;

    /* PCCC Command */
    uint8_t pccc_command;           /* CMD read, write etc. */
    uint8_t pccc_status;            /* STS 0x00 in request */
    uint16_le pccc_seq_num;          /* TNS transaction/sequence id */
    uint8_t pccc_function;          /* FNC sub-function of command */
    uint16_le pccc_offset;           /* offset of requested in total request */
    uint16_le pccc_transfer_size;    /* total number of words requested */
    //uint8_t pccc_data[ZLA_SIZE];   /* send_data for request */

    /* IOI path to DHRIO */
} END_PACK pccc_dhp_req;



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

    /* PCCC Reply */
    uint8_t reply_code;          /* 0xCB Execute PCCC Reply */
    uint8_t reserved;               /* 0x00 in reply */
    uint8_t general_status;         /* 0x00 for success */
    uint8_t status_size;            /* number of 16-bit words of extra status, 0 if success */

    /* PCCC Command Req Routing */
    uint8_t request_id_size;        /* ALWAYS 7 */
    uint16_le vendor_id;             /* Our CIP Vendor ID */
    uint32_le vendor_serial_number;  /* Our CIP Vendor Serial Number */

    /* PCCC Command */
    uint8_t pccc_command;           /* CMD read, write etc. */
    uint8_t pccc_status;            /* STS 0x00 in request */
    uint16_le pccc_seq_num;          /* TNSW transaction/connection sequence number */
    //uint8_t pccc_data[ZLA_SIZE];    /* data for PCCC response. */
} END_PACK pccc_dhp_resp;

#endif // __PROTOCOLS_AB_DEFS_H__


#ifndef __PROTOCOLS_AB_AB_COMMON_H__
#define __PROTOCOLS_AB_AB_COMMON_H__

typedef struct ab_tag_t *ab_tag_p;
#define AB_TAG_NULL ((ab_tag_p)NULL)

typedef struct ab_session_t *ab_session_p;
#define AB_SESSION_NULL ((ab_session_p)NULL)

typedef struct ab_request_t *ab_request_p;
#define AB_REQUEST_NULL ((ab_request_p)NULL)


int ab_tag_abort(ab_tag_p tag);
int ab_tag_status(ab_tag_p tag);


int ab_get_int_attrib(plc_tag_p tag, const char *attrib_name, int default_value);
int ab_set_int_attrib(plc_tag_p tag, const char *attrib_name, int new_value);

int ab_get_bit(plc_tag_p tag, int offset_bit);
int ab_set_bit(plc_tag_p tag, int offset_bit, int val);

uint64_t ab_get_uint64(plc_tag_p tag, int offset);
int ab_set_uint64(plc_tag_p tag, int offset, uint64_t val);

int64_t ab_get_int64(plc_tag_p tag, int offset);
int ab_set_int64(plc_tag_p tag, int offset, int64_t val);


uint32_t ab_get_uint32(plc_tag_p tag, int offset);
int ab_set_uint32(plc_tag_p tag, int offset, uint32_t val);

int32_t ab_get_int32(plc_tag_p tag, int offset);
int ab_set_int32(plc_tag_p tag, int offset, int32_t val);


uint16_t ab_get_uint16(plc_tag_p tag, int offset);
int ab_set_uint16(plc_tag_p tag, int offset, uint16_t val);

int16_t ab_get_int16(plc_tag_p tag, int offset);
int ab_set_int16(plc_tag_p tag, int offset, int16_t val);


uint8_t ab_get_uint8(plc_tag_p tag, int offset);
int ab_set_uint8(plc_tag_p tag, int offset, uint8_t val);

int8_t ab_get_int8(plc_tag_p tag, int offset);
int ab_set_int8(plc_tag_p tag, int offset, int8_t val);


double ab_get_float64(plc_tag_p tag, int offset);
int ab_set_float64(plc_tag_p tag, int offset, double val);

float ab_get_float32(plc_tag_p tag, int offset);
int ab_set_float32(plc_tag_p tag, int offset, float val);


//int ab_tag_destroy(ab_tag_p p_tag);
plc_type_t get_plc_type(attr attribs);
int check_cpu(ab_tag_p tag, attr attribs);
int check_tag_name(ab_tag_p tag, const char *name);
int check_mutex(int debug);
vector_p find_read_group_tags(ab_tag_p tag);

THREAD_FUNC(request_handler_func);

/* helpers for checking request status. */
int check_read_request_status(ab_tag_p tag, ab_request_p request);
int check_write_request_status(ab_tag_p tag, ab_request_p request);

#define rc_is_error(rc) (rc < PLCTAG_STATUS_OK)

#endif // __PROTOCOLS_AB_AB_COMMON_H__


#ifndef __PROTOCOLS_AB_AB_H__
#define __PROTOCOLS_AB_AB_H__

void ab_teardown(void);
int ab_init();
plc_tag_p ab_tag_create(attr attribs, void (*tag_callback_func)(int32_t tag_id, int event, int status, void *userdata), void *userdata);

#endif // __PROTOCOLS_AB_AB_H__


#ifndef __PROTOCOLS_AB_CIP_H__
#define __PROTOCOLS_AB_CIP_H__

//int cip_encode_path(ab_tag_p tag, const char *path);
int cip_encode_path(const char *path, int *needs_connection, plc_type_t plc_type, uint8_t **conn_path, uint8_t *conn_path_size, int *is_dhp, uint16_t *dhp_dest);

//~ char *cip_decode_status(int status);
int cip_encode_tag_name(ab_tag_p tag,const char *name);

#endif // __PROTOCOLS_AB_CIP_H__


#ifndef __PROTOCOLS_AB_ERROR_CODES_H__
#define __PROTOCOLS_AB_ERROR_CODES_H__

const char *decode_cip_error_short(uint8_t *data);
const char *decode_cip_error_long(uint8_t *data);
int decode_cip_error_code(uint8_t *data);

#endif // __PROTOCOLS_AB_ERROR_CODES_H__


#ifndef __PROTOCOLS_AB_PCCC_H__
#define __PROTOCOLS_AB_PCCC_H__


typedef enum {
               PCCC_FILE_UNKNOWN        = 0x00, /* UNKNOWN! */
               PCCC_FILE_ASCII          = 0x8e,
               PCCC_FILE_BCD            = 0x8f,
               PCCC_FILE_BIT            = 0x85,
               PCCC_FILE_BLOCK_TRANSFER = 0x00, /* UNKNOWN! */
               PCCC_FILE_CONTROL        = 0x88,
               PCCC_FILE_COUNTER        = 0x87,
               PCCC_FILE_FLOAT          = 0x8a,
               PCCC_FILE_INPUT          = 0x8c,
               PCCC_FILE_INT            = 0x89,
               PCCC_FILE_LONG_INT       = 0x91,
               PCCC_FILE_MESSAGE        = 0x92,
               PCCC_FILE_OUTPUT         = 0x8b,
               PCCC_FILE_PID            = 0x93,
               PCCC_FILE_SFC            = 0x00, /* UNKNOWN! */
               PCCC_FILE_STATUS         = 0x84,
               PCCC_FILE_STRING         = 0x8d,
               PCCC_FILE_TIMER          = 0x86
             } pccc_file_t;

typedef struct {
    pccc_file_t file_type;
    int file;
    int element;
    int sub_element;
    uint8_t is_bit;
    uint8_t bit;
    int element_size_bytes;
} pccc_addr_t;

int parse_pccc_logical_address(const char *file_address, pccc_addr_t *address);
int plc5_encode_address(uint8_t *data, int *size, int buf_size, pccc_addr_t *address);
int slc_encode_address(uint8_t *data, int *size, int buf_size, pccc_addr_t *address);

//int plc5_encode_tag_name(uint8_t *data, int *size, pccc_file_t *file_type, const char *name, int max_tag_name_size);
//int slc_encode_tag_name(uint8_t *data, int *size, pccc_file_t *file_type, const char *name, int max_tag_name_size);
uint8_t pccc_calculate_bcc(uint8_t *data,int size);
uint16_t pccc_calculate_crc16(uint8_t *data, int size);
const char *pccc_decode_error(uint8_t *error_ptr);
uint8_t *pccc_decode_dt_byte(uint8_t *data,int data_size, int *pccc_res_type, int *pccc_res_length);
int pccc_encode_dt_byte(uint8_t *data,int buf_size, uint32_t data_type, uint32_t data_size);

#endif // __PROTOCOLS_AB_PCCC_H__


#ifndef __PROTOCOLS_AB_SESSION_H__
#define __PROTOCOLS_AB_SESSION_H__

/* #define MAX_SESSION_HOST    (128) */

#define SESSION_DEFAULT_TIMEOUT (2000)

#define MAX_PACKET_SIZE_EX  (44 + 4002)

#define SESSION_MIN_REQUESTS    (10)
#define SESSION_INC_REQUESTS    (10)


struct ab_session_t {
//    int status;
    int failed;
    int on_list;

    /* gateway connection related info */
    char *host;
    int port;
    char *path;
    sock_p sock;

    /* connection variables. */
    int use_connected_msg;
    int only_use_old_forward_open;
    uint16_t max_payload_guess;
    uint32_t orig_connection_id;
    uint32_t targ_connection_id;
    uint16_t conn_seq_num;
    uint16_t conn_serial_number;

    plc_type_t plc_type;

    uint16_t max_payload_size;
    uint8_t *conn_path;
    uint8_t conn_path_size;
    uint16_t dhp_dest;
    int is_dhp;

    int connection_group_id;

    /* registration info */
    uint32_t session_handle;

    /* Sequence ID for requests. */
    uint64_t session_seq_id;

    /* list of outstanding requests for this session */
    vector_p requests;

    /* data for receiving messages */
    uint64_t resp_seq_id;
    uint32_t data_offset;
    uint32_t data_capacity;
    uint32_t data_size;
    uint8_t data[MAX_PACKET_SIZE_EX];

    uint64_t packet_count;

    thread_p handler_thread;
    volatile int terminating;
    mutex_p mutex;
    cond_p wait_cond;

    /* disconnect handling */
    int auto_disconnect_enabled;
    int auto_disconnect_timeout_ms;
};


struct ab_request_t {
    /* used to force interlocks with other threads. */
    lock_t lock;

    int status;

    /* flags for communicating with background thread */
    int resp_received;
    int abort_request;

    /* debugging info */
    int tag_id;

    /* allow requests to be packed in the session */
    int allow_packing;
    int packing_num;

    /* time stamp for debugging output */
    int64_t time_sent;

    /* used by the background thread for incrementally getting data */
    int request_size; /* total bytes, not just data */
    int request_capacity;
    uint8_t *data;
};



uint64_t session_get_new_seq_id_unsafe(ab_session_p sess);
uint64_t session_get_new_seq_id(ab_session_p sess);

int session_startup();
void session_teardown();

int session_find_or_create(ab_session_p *session, attr attribs);
int session_get_max_payload(ab_session_p session);
int session_create_request(ab_session_p session, int tag_id, ab_request_p *request);
int session_add_request(ab_session_p sess, ab_request_p req);

#endif // __PROTOCOLS_AB_SESSION_H__


#ifndef __PROTOCOLS_AB_TAG_H__
#define __PROTOCOLS_AB_TAG_H__

/* do these first */
#define MAX_TAG_NAME        (260)
#define MAX_TAG_TYPE_INFO   (64)
#define MAX_CONN_PATH       (260)   /* 256 plus padding. */

typedef enum {
    AB_TYPE_BOOL,
    AB_TYPE_BOOL_ARRAY,
    AB_TYPE_CONTROL,
    AB_TYPE_COUNTER,
    AB_TYPE_FLOAT32,
    AB_TYPE_FLOAT64,
    AB_TYPE_INT8,
    AB_TYPE_INT16,
    AB_TYPE_INT32,
    AB_TYPE_INT64,
    AB_TYPE_STRING,
    AB_TYPE_SHORT_STRING,
    AB_TYPE_TIMER,
    AB_TYPE_TAG_ENTRY,  /* not a real AB type, but a pseudo type for AB's internal tag entry. */
    AB_TYPE_TAG_UDT,    /* as above, but for UDTs. */
    AB_TYPE_TAG_RAW     /* raw CIP tag */
} elem_type_t;


struct ab_tag_t {
    /*struct plc_tag_t p_tag;*/
    TAG_BASE_STRUCT;

    /* how do we talk to this device? */
    plc_type_t plc_type;

    /* pointers back to session */
    ab_session_p session;
    int use_connected_msg;

    /* this contains the encoded name */
    uint8_t encoded_name[MAX_TAG_NAME];
    int encoded_name_size;

//    const char *read_group;

    /* storage for the encoded type. */
    uint8_t encoded_type_info[MAX_TAG_TYPE_INFO];
    int encoded_type_info_size;

    /* number of elements and size of each in the tag. */
    pccc_file_t file_type;
    elem_type_t elem_type;

    int elem_count;
    int elem_size;

    int special_tag;

    /* Used for standard tags. How much data can we send per packet? */
    int write_data_per_packet;

    /* used for listing tags. */
    uint32_t next_id;

    /* used for UDT tags. */
    uint8_t udt_get_fields;
    uint16_t udt_id;

    /* requests */
    int pre_write_read;
    int first_read;
    ab_request_p req;
    int offset;

    int allow_packing;

    /* flags for operations */
    int read_in_progress;
    int write_in_progress;
    /*int connect_in_progress;*/
};

#endif // __PROTOCOLS_AB_TAG_H__


#ifndef __PROTOCOLS_AB_EIP_CIP_SPECIAL_H__
#define __PROTOCOLS_AB_EIP_CIP_SPECIAL_H__

/* tag creation helpers */
//int setup_special_cip_tag(ab_tag_p tag, const char *name);
int setup_raw_tag(ab_tag_p tag);
int setup_tag_listing_tag(ab_tag_p tag, const char *name);
int setup_udt_tag(ab_tag_p tag, const char *name);

#endif // __PROTOCOLS_AB_EIP_CIP_SPECIAL_H__


#ifndef __PROTOCOLS_AB_EIP_CIP_H__
#define __PROTOCOLS_AB_EIP_CIP_H__

/* tag listing helpers */
int setup_tag_listing(ab_tag_p tag, const char *name);

#endif // __PROTOCOLS_AB_EIP_CIP_H__


#ifdef __cplusplus
}
#endif


#endif // __LIBPLCTAG_INTERNAL_H__