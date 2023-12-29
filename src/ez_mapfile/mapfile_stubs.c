/**************************************************************************/
/*                                                                        */
/*  Copyright (c) 2023 OCamlPro SAS                                       */
/*                                                                        */
/*  All rights reserved.                                                  */
/*  This file is distributed under the terms of the GNU Lesser General    */
/*  Public License version 2.1, with the special exception on linking     */
/*  described in the LICENSE.md file in the root directory.               */
/*                                                                        */
/*                                                                        */
/**************************************************************************/

#ifdef __APPLE__
#define O_LARGEFILE 0
#else
#define _LARGEFILE64_SOURCE
#endif

#include "caml/mlvalues.h"
#include "caml/fail.h"
#include "caml/alloc.h"
#include "caml/gc.h"
#include "caml/memory.h"
#include "caml/bigarray.h"

#ifndef Caml_black
/* we are in version 5 */
#include "caml/shared_heap.h"
#define Caml_black NOT_MARKABLE
#endif

#if defined(_WIN32) || defined(_WIN64)
#define SHARED_MEMORY_FOR_WINDOWS
#endif

#ifdef SHARED_MEMORY_FOR_WINDOWS

#include <windows.h>
#include <conio.h>
#include <tchar.h>

#ifdef _MSC_VER

typedef unsigned __int8 uint8_t;
typedef unsigned __int16 uint16_t;
typedef unsigned __int32 uint32_t;

#else
#include <stdint.h>
#endif

#define SHARED_FD HANDLE
#define CLOSE_SHARED_FD CloseHandle
#define STORE_FD(x) (value)(x)
#define RESTORE_FD(x) (SHARED_FD)(x)
#define caml_stat _stat

#else /* FOR LINUX */

#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>

#include <sys/mman.h> /* for mmap */
#include <arpa/inet.h>  /* for htonl */

#define SHARED_FD int
#define STORE_FD(x) Val_int(x)
#define RESTORE_FD(x) Int_val(x)
#define CLOSE_SHARED_FD close
#define caml_stat stat
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

typedef struct {
  int fd ;
  unsigned char* addr;
  uint64_t map_size ;
  uint64_t current_size ;
} mapfile_t ;

#define INITIAL_SIZE_OFFSET 20

static void ocp_mapfile_increase_file(mapfile_t* db, uint64_t expected_size)
{
  expected_size += 10 ; /* always add at least 10 bytes at the end */
  if( expected_size > db->current_size ){
    off_t ret ;
    char buf[2];
    uint64_t final_size =
      (1 + (expected_size >> INITIAL_SIZE_OFFSET)) << INITIAL_SIZE_OFFSET ;
    if( final_size > db->map_size ){
      caml_failwith("Mapfile: map_size exceeeded");
    }
    /*    fprintf(stderr, "INCREASING SIZE TO %Ld > %Ld\n",
          final_size, expected_size); */
    ret = lseek( db->fd, final_size-1, SEEK_SET);
    if( ret < 0 ){ caml_failwith("ocp_mapfile_increase_file: bad lseek"); }
    buf[0] = 0;
    buf[1] = 0;
    ret = write(db->fd, buf, 1);
    if( ret < 0 ){ caml_failwith("ocp_mapfile_increase_file: bad write"); }
    db->current_size = final_size;
    /* fprintf(stderr, "INCREASED !\n"); */
  }
}

static void ocp_mapfile_check_size(mapfile_t* db, uint64_t expected_size)
{
  if( expected_size > db->current_size){
    db->current_size = lseek( db->fd, 0, SEEK_END );
    if( expected_size > db->current_size ){
      caml_failwith("Mapfile.read: file too short");
    }
  }
}


value ocp_mapfile_openfile_c (
                           value filename_v ,
                           value read_only_v ,
                           value map_size_v ,
                           value create_v
                           )
{
  CAMLparam2( filename_v, map_size_v);
  CAMLlocal1( db_v );
  size_t map_size = Int64_val( map_size_v );
  char *addr ;
  int read_only = Long_val( read_only_v );
  int create = Long_val ( create_v );
  const char* filename = String_val( filename_v );
  int fd ;
  int map_flags;
  int open_flags;
  int open_mode = 420; /* 0o644 */
  uint64_t next_position;
  mapfile_t* db = NULL;
  uint64_t initial_size;
#ifdef SHARED_MEMORY_FOR_WINDOWS
  HANDLE handle;
  SHARED_FD fmap;
  LARGE_INTEGER li;
#else
  open_flags = O_LARGEFILE;
#endif

  // fprintf(stderr, "ocp_mapfile_openfile_c...\n%!");
  if( read_only )
    open_flags |= O_RDONLY ;
  else
    open_flags |= O_RDWR ;
  if( create ) open_flags |= O_CREAT;
  fd = open( filename, open_flags, open_mode );
  if( fd < 0 ) { caml_failwith("Mapfile.opendir: open failed"); }
  initial_size = lseek( fd, 0, SEEK_END );
  /*
  fprintf(stderr, "map_size %Ld\n", map_size);
  fprintf(stderr, "initial_size %Ld\n", initial_size);
  */
  if( initial_size > map_size ){
    close(fd);
    caml_failwith("EzMapfile.Mapfile.openfile: map_size exceeeded");
  }

#ifdef SHARED_MEMORY_FOR_WINDOWS
  handle = (HANDLE) _get_osfhandle (fd);
  li.QuadPart = map_size ;
  fmap = CreateFileMapping(handle, NULL,
			   read_only ? PAGE_READONLY : PAGE_READWRITE,
			   li.HighPart, li.LowPart, NULL);
  addr = MapViewOfFile(fmap,
		       read_only ? FILE_MAP_COPY : FILE_MAP_WRITE,
		       0, 0, map_size);
  CloseHandle(fmap);
#else
  map_flags = PROT_READ ;
  if ( ! read_only ) map_flags |= PROT_WRITE ;
  addr = mmap(NULL,
              map_size,
              map_flags,
              MAP_SHARED,
              fd,
              0);
#endif
  if( addr == NULL ){ caml_failwith("Mapfile.openfile: mmap failed"); }

  db = (mapfile_t*)malloc( sizeof(mapfile_t) );
  db->fd = fd;
  db->addr = addr;
  db->map_size = map_size;
  if( create ){
    db->current_size = 0;
    /* ocp_mapfile_increase_file(db, sizeof(file_t) ); */
  } else {
    db->current_size = initial_size;
  }
  /* fprintf(stderr, "current_size = %Ld\n", db->current_size); */
  db_v = caml_alloc( 1 , Abstract_tag );
  Field (db_v, 0) = (value) db;
  // fprintf(stderr, "ocp_mapfile_openfile_c...DONE\n%!");
  CAMLreturn( db_v ) ;
}

value ocp_mapfile_write_fixed_string_c ( value db_v ,
                                  value off_v ,
                                  value contents_v ,
                                  value pos_v ,
                                  value len_v )
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  uint64_t off = Long_val( off_v );
  const char* contents = String_val( contents_v );
  uint64_t pos = Long_val ( pos_v );
  uint64_t len = Long_val ( len_v );
  uint64_t contents_len = caml_string_length( contents_v );

  if( db-> fd < 0 ){
    caml_failwith("Mapfile.write_fixed_string: already closed");
  }
  if( pos + len > contents_len ){
    caml_failwith("Mapfile.write_fixed_string");
  }

  ocp_mapfile_increase_file( db, off + len );
  memcpy( db->addr + off,
          contents + pos,
          len);
  return Val_long( off + len );
}

value ocp_mapfile_write_string_c ( value db_v ,
                                  value off_v ,
                                  value contents_v ,
                                  value pos_v ,
                                  value len_v )
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  uint64_t off = Long_val( off_v );
  const char* contents = String_val( contents_v );
  uint64_t pos = Long_val ( pos_v );
  uint64_t len = Long_val ( len_v );
  uint64_t contents_len = caml_string_length( contents_v );
  unsigned char* addr = db->addr;
  uint64_t initial_off = off;

  if( db-> fd < 0 ){
    caml_failwith("Mapfile.write_string: already closed");
  }
  if( pos + len > contents_len ){
    caml_failwith("Mapfile.write_string");
  }

  ocp_mapfile_increase_file( db, off + 8 + len );
  {
    uint64_t size = len;
    while( size >= 128 ){
      addr[off++] = size & 0x7f ;
      size = size >> 7;
    }
    addr[off++] = size | 0x80 ;
  }

  memcpy( db->addr + off,
          contents + pos,
          len);

  off += len;
  if( off - initial_off < 8 ) off = initial_off + 8;
  return Val_long( off );
}


value ocp_mapfile_write_int_variable_c ( value db_v ,
					 value off_v ,
					 value len_v )
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  uint64_t off = Long_val( off_v );
  uint64_t len = Long_val ( len_v );
  unsigned char* addr = db->addr;
  uint64_t initial_off = off;

  if( db-> fd < 0 ){
    caml_failwith("Mapfile.write_string: already closed");
  }

  ocp_mapfile_increase_file( db, off + 8 );
  {
    uint64_t size = len;
    while( size >= 128 ){
      addr[off++] = size & 0x7f ;
      size = size >> 7;
    }
    addr[off++] = size | 0x80 ;
  }
  if( off - initial_off < 8 ) off = initial_off + 8;
  
  return Val_long( off );
}

value ocp_mapfile_write_fixed_bigstring_c ( value db_v ,
                                  value off_v ,
                                  value contents_v ,
                                  value pos_v ,
                                  value len_v )
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  uint64_t off = Long_val( off_v );
  unsigned char* contents = Caml_ba_data_val( contents_v );
  uint64_t pos = Long_val ( pos_v );
  uint64_t len = Long_val ( len_v );
  uint64_t contents_len = Caml_ba_array_val( contents_v )->dim[0];

  if( db-> fd < 0 ){
    caml_failwith("Mapfile.write_fixed_bigstring: already closed");
  }
  if( pos + len > contents_len ){
    caml_failwith("Mapfile.write_fixed_bigstring");
  }

  ocp_mapfile_increase_file( db, off + len );
  memcpy( db->addr + off,
          contents + pos,
          len);
  return Val_long( off + len );
}

value ocp_mapfile_write_bigstring_c ( value db_v ,
                                  value off_v ,
                                  value contents_v ,
                                  value pos_v ,
                                  value len_v )
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  uint64_t off = Long_val( off_v );
  unsigned char* contents = Caml_ba_data_val( contents_v );
  uint64_t pos = Long_val ( pos_v );
  uint64_t len = Long_val ( len_v );
  uint64_t contents_len = Caml_ba_array_val( contents_v )->dim[0];
  unsigned char* addr = db->addr;
  uint64_t initial_off = off;
  
  if( db-> fd < 0 ){
    caml_failwith("Mapfile.write_bigstring: already closed");
  }
  if( pos + len > contents_len ){
    caml_failwith("Mapfile.write_bigstring");
  }

  ocp_mapfile_increase_file( db, off + 8 + len );
  {
    uint64_t size = len;
    while( size >= 128 ){
      addr[off++] = size & 0x7f ;
      size = size >> 7;
    }
    addr[off++] = size | 0x80 ;
  }
  memcpy( db->addr + off,
          contents + pos,
          len);

  off += len;
  if( off - initial_off < 8 ) off = initial_off + 8;
  return Val_long( off );
}

value ocp_mapfile_size_c( value db_v )
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  if( db-> fd < 0 ){
    caml_failwith("Mapfile.size: already closed");
  }
  return Val_long( db->current_size );
}

value ocp_mapfile_commit_c ( value db_v, value sync_v )
{
  int ret;
  int sync = Long_val( sync_v );
  int flag;

  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  if( db-> fd < 0 ){
    caml_failwith("Mapfile.commit: already closed");
  }
#ifdef SHARED_MEMORY_FOR_WINDOWS
  FlushViewOfFile ( db->addr, db->map_size);
#else
  flag = sync ? MS_SYNC : MS_ASYNC ;
  ret = msync( db->addr, db->current_size, flag );
  if( ret < 0 ){
    caml_failwith("Mapfile.commit: bad msync");
  }
#endif
  return Val_unit ;
}

value ocp_mapfile_close_c ( value db_v )
{
  int ret;

  mapfile_t * db = (mapfile_t *) Field( db_v, 0);

  if( db-> fd < 0 ){
    caml_failwith("Mapfile.close: already closed");
  }

#ifdef SHARED_MEMORY_FOR_WINDOWS
  UnmapViewOfFile( db->addr );
#else
  ret = msync( db->addr, db->current_size, MS_SYNC );
  if( ret < 0 ){
    caml_failwith("Mapfile.close: bad msync");
  }
  munmap (db->addr, db->map_size);
#endif
  close( db->fd );
  db->fd = -1;
  return Val_unit ;
}

value ocp_mapfile_read_int_variable_c ( value db_v ,
                                value off_v )
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  uint64_t off = Long_val( off_v );
  unsigned char* addr = db->addr;
  uint64_t len = 0;

  {
    unsigned char c;
    uint64_t lsl = 0;
    ocp_mapfile_check_size( db, off + 8 );
    c = addr[off++];
    while( c < 128 ){
      len += c << lsl;
      lsl += 7;
      c = addr[off++];
    }
    len += ( c & 0x7f ) << lsl;
  }

  return Val_long( len );
}

value ocp_mapfile_after_string_c ( value db_v ,
                                value off_v )
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  uint64_t off = Long_val( off_v );
  unsigned char* addr = db->addr;
  uint64_t len = 0;
  uint64_t initial_off = off ;

  {
    unsigned char c;
    uint64_t lsl = 0;
    ocp_mapfile_check_size( db, off + 8 );
    c = addr[off++];
    while( c < 128 ){
      len += c << lsl;
      lsl += 7;
      c = addr[off++];
    }
    len += ( c & 0x7f ) << lsl;
  }
  off += len ;
  if( off - initial_off < 8 ) off = initial_off + 8;

  return Val_long( off );
}

value ocp_mapfile_read_fixed_bigstring_c( value db_v, value off_v, value len_v )
{
  CAMLparam1( db_v );
  CAMLlocal1( ret_v );
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  unsigned char* addr = db->addr;
  uint64_t off = Long_val( off_v );
  uint64_t len = Long_val( len_v );

  ocp_mapfile_check_size( db, off + len );
  ret_v = caml_ba_alloc_dims(CAML_BA_UINT8 | CAML_BA_C_LAYOUT, 1,
                             addr + off, len) ;
  CAMLreturn( ret_v );
}

value ocp_mapfile_read_bigstring_c( value db_v, value off_v )
{
  CAMLparam1( db_v );
  CAMLlocal1( ret_v );
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  unsigned char* addr = db->addr;
  uint64_t off = Long_val( off_v );
  uint64_t len = 0 ;

  {
    unsigned char c;
    uint64_t lsl = 0;
    ocp_mapfile_check_size( db, off + 8 );
    c = addr[off++];
    while( c < 128 ){
      len += c << lsl;
      lsl += 7;
      c = addr[off++];
    }
    len += ( c & 0x7f ) << lsl;
  }

  ocp_mapfile_check_size( db, off + len );
  ret_v = caml_ba_alloc_dims(CAML_BA_UINT8 | CAML_BA_C_LAYOUT, 1,
                             addr + off, len) ;
  CAMLreturn( ret_v );
}

value ocp_mapfile_truncate_c(mapfile_t* db_v, uint64_t wanted_size_v)
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  uint64_t wanted_size = Long_val( wanted_size_v );

  if( wanted_size < db->current_size ){
    int ret = ftruncate(db->fd, wanted_size);
    if( ret < 0 ){ caml_failwith("ocp_mapfile_truncate: bad ftruncate"); }
    db->current_size = wanted_size;
  }
  return Val_unit;
}


value ocp_mapfile_read_fixed_bytes_c( value db_v, value off_v,
				      value bytes_v, value pos_v, value len_v )
{
  char* contents = Bytes_val( bytes_v );
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  unsigned char* addr = db->addr;
  uint64_t off = Long_val( off_v );
  uint64_t pos = Long_val( pos_v );
  uint64_t len = Long_val( len_v );

  if (pos+len>caml_string_length(bytes_v)){
	  caml_failwith("ocp_mapfile_read_fixed_bytes_c: bytes is too small");
  }
  ocp_mapfile_check_size( db, off + len );
  memcpy( contents + pos,
	  db->addr + off,
	  len);
  return Val_unit;
}

value ocp_mapfile_read_bytes_c( value db_v, value off_v,
				value bytes_v, value pos_v
	)
{
  char* contents = Bytes_val ( bytes_v );
  uint64_t contents_len = caml_string_length ( bytes_v );
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  unsigned char* addr = db->addr;
  uint64_t off = Long_val( off_v );
  uint64_t len = 0 ;
  uint64_t pos = Long_val (pos_v);

  {
    unsigned char c;
    uint64_t lsl = 0;
    ocp_mapfile_check_size( db, off + 8 );
    c = addr[off++];
    while( c < 128 ){
      len += c << lsl;
      lsl += 7;
      c = addr[off++];
    }
    len += ( c & 0x7f ) << lsl;
  }

  if (pos+len>caml_string_length(bytes_v)){
	  caml_failwith("ocp_mapfile_read_bytes_c: bytes is too small");
  }
  ocp_mapfile_check_size( db, off + len );
  memcpy( contents + pos,
	  db->addr + off,
	  len);

  return Val_long(len);
}

#define htonll(x) ((1==htonl(1)) ? (x) : ((uint64_t)htonl((x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32))
#define ntohll(x) ((1==ntohl(1)) ? (x) : ((uint64_t)ntohl((x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32))


int size_of_int_format(int format)
{
	switch ( format ){
	case 0: /* INT8 */
		return 1;
	case 1: /* INT16NA */
		return 2;
	case 2: /* INT16NO */
		return 2;
	case 3: /* INT32NA */
		return 4;
	case 4: /* INT32NO */
		return 4;
	case 5: /* INT64NA */
		return 8;
	case 6: /* INT64NO */
		return 8;
	}
	exit (2);
}

value ocp_mapfile_write_int_c( value db_v, value off_v,
				value format_v, value int_v)
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  uint64_t off = Long_val( off_v );
  int64_t val = Long_val ( int_v );
  int format = Int_val (format_v);
  int size = size_of_int_format ( format ) ;
  unsigned char* addr = db->addr + off;
  int off_final = off+size;
  ocp_mapfile_increase_file( db, off_final );
  switch (format){
  case 0: /* INT8 */
	  *(char*)(addr) = val;
	  break;
  case 1: /* INT16NA */
	  *(int16_t*)(addr) = val;
	  break;
  case 2: /* INT16NO */
	  *(int16_t*)(addr) = htons (val);
	  break;
  case 3: /* INT32NA */
	  *(int32_t*)(addr) = val;
	  break;
  case 4: /* INT32NO */
	  *(int32_t*)(addr) = htonl (val);
	  break;
  case 5: /* INT64NA */
	  *(int64_t*)(addr) = val;
	  break;
  case 6: /* INT64NO */
	  *(int64_t*)(addr) = htonll (val);
	  break;
  }
  return Val_int(off_final);
}

value ocp_mapfile_read_int_c( value db_v, value off_v, value format_v)
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  uint64_t off = Long_val( off_v );
  int format = Int_val (format_v);
  int size = size_of_int_format ( format ) ;
  unsigned char* addr = db->addr + off;
  int64_t val = 0;

  ocp_mapfile_check_size( db, off + size );
  switch (format){
  case 0: /* INT8 */
	  val = * (char*)(addr);
	  break;
  case 1: /* INT16NA */
	  val = * (int16_t*)(addr);
	  break;
  case 2: /* INT16NO */
	  val = (int16_t) ntohl (* (int16_t*)(addr));
	  break;
  case 3: /* INT32NA */
	  val = * (int32_t*)(addr);
	  break;
  case 4: /* INT32NO */
	  val = (int32_t) ntohl (* (int32_t*)(addr));
	  break;
  case 5: /* INT64NA */
	  val = * (int64_t*)(addr);
	  break;
  case 6: /* INT64NO */
	  val = (int64_t) ntohll (* (int64_t*)(addr));
	  break;
  }
  return Val_long (val);
}

value ocp_mapfile_write_ocaml_string_c( value db_v, value off_v, value s_v)
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  uint64_t off = Long_val( off_v );
  unsigned char* addr = db->addr + off;
  int wosize = Wosize_val (s_v);
  int bhsize = 8 + wosize * 8;
  ocp_mapfile_increase_file( db, off + bhsize );

/* For 5.*, Caml_black should be replaced by  NOT_MARKABLE); */
  *(value*)addr = Make_header (wosize, String_tag, Caml_black); 

  memcpy (addr+8,
	   String_val(s_v),
	   bhsize-8);
  return off + bhsize;
}

value ocp_mapfile_read_ocaml_string_c( value db_v, value off_v)
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  uint64_t off = Long_val( off_v ) + 8;
  unsigned char* addr = db->addr + off;
  uint64_t size;

  ocp_mapfile_check_size( db, off );
  size = Bosize_val (addr);
  ocp_mapfile_check_size( db, off + size );
  return (value)addr ;
}

value ocp_mapfile_after_ocaml_string_c( value db_v, value off_v)
{
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  uint64_t off = Long_val( off_v ) + 8;
  unsigned char* addr = db->addr + off;
  uint64_t size;

  ocp_mapfile_check_size( db, off );
  size = Bosize_val (addr);
  return (value) (addr + size) ;
}

value ocp_mapfile_read_string_c( value db_v, value off_v )
{
  CAMLparam1( db_v );
  CAMLlocal1( ret_v );
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  unsigned char* addr = db->addr;
  uint64_t off = Long_val( off_v );
  uint64_t len = 0 ;

  {
    unsigned char c;
    uint64_t lsl = 0;
    ocp_mapfile_check_size( db, off + 8 );
    c = addr[off++];
    while( c < 128 ){
      len += c << lsl;
      lsl += 7;
      c = addr[off++];
    }
    len += ( c & 0x7f ) << lsl;
  }

  ocp_mapfile_check_size( db, off + len );
  ret_v = caml_alloc_string(len);
  memcpy (Bytes_val(ret_v),
	   addr+off,
	   len);
  
  CAMLreturn( ret_v );
}

value ocp_mapfile_read_fixed_string_c( value db_v, value off_v, value len_v )
{
  CAMLparam1( db_v );
  CAMLlocal1( ret_v );
  mapfile_t * db = (mapfile_t *) Field( db_v, 0);
  unsigned char* addr = db->addr;
  uint64_t off = Long_val( off_v );
  uint64_t len = Long_val( len_v );

  ocp_mapfile_check_size( db, off + len );
  ret_v = caml_alloc_string(len);
  memcpy (Bytes_val(ret_v),
	   addr+off,
	   len);
  CAMLreturn( ret_v );
}
