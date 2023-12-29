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

let custom_size { size ; _ } = size
let custom_encode { encode ; _ } v = encode v
let custom_decode { decode ; _ } s = decode s
let magic (type s) (t : s t) =
  match t with
  | StringEnc { magic ; _ } -> magic ^ "STRING"
  | IntEnc { magic ; size ; _ } ->
      magic ^
      match size with
      | Int8 -> "INT8"
      | Int16NA -> "INT16NA"
      | Int16NO -> "INT16NO"
      | Int32NA -> "INT32NA"
      | Int32NO -> "INT32NO"
      | Int63NA -> "INT63NA"
      | Int63NO -> "INT63NO"

let size_of_int_format = function
  | Int8 -> 1
  | Int16NA | Int16NO -> 2
  | Int32NA | Int32NO -> 4
  | Int63NA | Int63NO -> 8

let id x = x
let create_string ~size ~magic ~encode ~decode =
  StringEnc { size ; encode ; decode ; magic }
let create_int ~size ~magic ~encode ~decode =
  IntEnc { size  ; encode ; decode ; magic }

let size (type s) ( t : s t) =
  match t with
  | IntEnc custom ->
      Some ( size_of_int_format custom.size )
  | StringEnc custom ->
      match custom.size with
      | None -> None
      | Some size -> Some size

let string ?size magic =
  create_string ~size ~magic ~encode:id ~decode:id

let int8 magic =
  create_int ~size:Int8 ~magic ~encode:id ~decode:id
let int16no magic =
  create_int ~size:Int16NO ~magic ~encode:id ~decode:id
let int16na magic =
  create_int ~size:Int16NA ~magic ~encode:id ~decode:id
let int32no magic =
  create_int ~size:Int32NO ~magic ~encode:id ~decode:id
let int32na magic =
  create_int ~size:Int32NA ~magic ~encode:id ~decode:id
let int63no magic =
  create_int ~size:Int63NO ~magic ~encode:id ~decode:id
let int63na magic =
  create_int ~size:Int63NA ~magic ~encode:id ~decode:id

let char_of_int8 n =
  if n < 0 && n >= -128 then
    char_of_int (n + 256)
  else
    char_of_int n

let int8_of_char c =
  let n = int_of_char c in
  if n > 127 then
    n - 256
  else
    n

let bytes_of_int int_format x =
  let b = Bytes.create (size_of_int_format int_format) in
  begin
    match int_format with
    | Int8 ->
        Bytes.set b 0 (char_of_int8 x)
    | Int16NA ->
        EndianBytes.NativeEndian.set_int16 b 0 x
    | Int16NO ->
        EndianBytes.BigEndian.set_int16 b 0 x
    | Int32NA ->
        EndianBytes.NativeEndian.set_int32 b 0 @@ Int32.of_int x
    | Int32NO ->
        EndianBytes.BigEndian.set_int32 b 0 @@ Int32.of_int x
    | Int63NA ->
        EndianBytes.NativeEndian.set_int64 b 0 @@ Int64.of_int x
    | Int63NO ->
        EndianBytes.BigEndian.set_int64 b 0 @@ Int64.of_int x
  end;
  b

let encode t x =
  match t with
  | StringEnc custom -> custom_encode custom x
  | IntEnc custom ->
      let x = custom_encode custom x in
      let b = bytes_of_int custom.size x in
      Bytes.unsafe_to_string b

let int_of_string int_format s =
  match int_format with
  | Int8 ->
      String.get s 0 |> int8_of_char
  | Int16NO ->
      EndianString.BigEndian.get_int16 s 0
  | Int16NA ->
      EndianString.NativeEndian.get_int16 s 0
  | Int32NA ->
      EndianString.NativeEndian.get_int32 s 0 |> Int32.to_int
  | Int32NO ->
      EndianString.BigEndian.get_int32 s 0 |> Int32.to_int
  | Int63NA ->
      EndianString.NativeEndian.get_int64 s 0 |> Int64.to_int
  | Int63NO ->
      EndianString.BigEndian.get_int64 s 0 |> Int64.to_int

let decode t s =
  match t with
  | StringEnc custom -> custom_decode custom s
  | IntEnc custom ->
      let x = int_of_string custom.size s in
      custom_decode custom x
