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

open Types

include FILE

(* FileSequence.length t: number of items in file, i.e. not removed
*)

type pos = int

(*

The file contains:
* a magic string, composed of `magic ^ encoding.magic`
* 8 bytes : the final position in big-endian
* 8 bytes : the number of items in big-endian
* items[...]

Format is optimised depending on the FileEncoding, to store either the
   size and the content (string encoding without specified size), or
   only the content (string encoding with fixed size, or integer)

*)

(* ?magic="EZMAPSEQ"

   ?zero: if specified, `get t 0` will return this value, and `add t
   zero` will return 0. If `zero` is specified, iterators will not work
   correctly, and neither `init`.
*)

val create :
  ?magic:string ->
  ?config:FileEngine.config ->
  ?counters:int ->
  ?async:bool ->
  mode ->
  'value FileEncoding.t ->
  ?zero:'value  ->
  string ->
  'value t

val add : 'value t -> 'value -> pos
(* add `n` times a value. only useful for fixed-size encodings *)
val init : 'value t -> int -> 'value -> unit

(* Impact on`length t` (decrement) and `stats t`. Also, beware that iterators will still
   iter on the value even if it has no usage. Using `set` on this position will not
   change the fact that it is of no usage.
*)
val add_no_usage : 'value t -> pos -> unit

val get : 'value t -> pos -> 'value

(* only with constant encoding, otherwise error SetWithVariableSizeEncoding *)
val set : 'value t -> pos -> 'value -> unit

(* Iterator functions *)
val first_pos : 'value t -> pos
val end_pos : 'value t -> pos
val next_pos : 'value t -> pos -> pos

val iter : ?begin_pos:pos -> ?end_pos:pos -> ('value -> unit) -> 'value t -> unit

type 'value iterator

(* if begin_pos is not specified, then use first_pos of file.
   if end_pos is not specified, then check end_pos at every get
*)
val iterator_create : ?begin_pos:pos -> ?end_pos:pos -> 'value t -> 'value iterator
(* returns `Some v` for the next value, or `None` if `end_pos` is reached *)
val iterator_get : 'value iterator -> 'value option

type stats = {
  end_pos : int ;
  length : int ;
  no_usage : int ;
}

val stats : 'value t -> stats
val encoding : 'value t -> 'value FileEncoding.t

val zero : 'value t -> 'value option
val magic : 'value t -> string
