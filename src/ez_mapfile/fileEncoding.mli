(**************************************************************************)
(*                                                                        *)
(*  Copyright (c) 2023 OCamlPro SAS                                       *)
(*                                                                        *)
(*  All rights reserved.                                                  *)
(*  This file is distributed under the terms of the GNU Lesser General    *)
(*  Public License version 2.1, with the special exception on linking     *)
(*  described in the LICENSE.md file in the root directory.               *)
(*                                                                        *)
(*                                                                        *)
(**************************************************************************)

type int_format =
  | Int8
  | Int16NA
  | Int16NO
  | Int32NA  (* native format *)
  | Int32NO  (* network order = big endian *)
  | Int63NA
  | Int63NO

type ('a,'format, 'size) custom = {
  encode : ('a -> 'format);
  decode : ('format -> 'a);
  size : 'size ;
  magic : string ;
}

type _ t =
  | StringEnc : ('a, string, int option) custom -> 'a t
  | IntEnc :    ('a, int, int_format) custom -> 'a t


val create_int :
  size:int_format ->
  magic:string ->
  encode:('a -> int) ->
  decode:(int -> 'a) ->
  'a t

val create_string :
  size:int option ->
  magic:string ->
  encode:('a -> string) ->
  decode:(string -> 'a) ->
  'a t

val custom_size :   ('a,'format,'size) custom -> 'size
val custom_encode : ('a,'format,'size) custom -> 'a -> 'format
val custom_decode : ('a,'format,'size) custom -> 'format -> 'a

val magic : 'a t -> string

val size : 'a t -> int option
val string : ?size:int -> (* magic *) string -> string t

val int8 : (* magic *) string -> int t
val int16na : (* magic *) string -> int t
val int16no : (* magic *) string -> int t
val int32na : (* magic *) string -> int t
val int32no : (* magic *) string -> int t
val int63na : (* magic *) string -> int t
val int63no : (* magic *) string -> int t

val encode : 'a t -> 'a -> string
val decode : 'a t -> string -> 'a

val size_of_int_format :  int_format -> int
val int_of_string : int_format -> string -> int
val bytes_of_int : int_format -> int -> bytes

(* differs from int_of_char for [128..255] that becomes [-128..-1] *)
val int8_of_char : char -> int
val char_of_int8 : int -> char
