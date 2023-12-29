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

module type FILE = sig
  type 'a t

  (* `close ?again t`: close the file, potentially msync before. Any later
     operation on the file will fail. *)
  val close : ?again:bool -> 'a t -> unit
  (* `closed t`: return true if the file has been closed. *)
  val closed : 'a t -> bool

  (* `rename t new_name`: close the file if needed, rename it to
     `new_name`, and reopen it if it was not closed. *)
  val rename : 'a t -> string -> unit

  (* `delete t`: close the file if needed, and delete the associated
     files on disk. *)
  val delete : 'a t -> unit

  (* `config t`: return the config of the file (on-disk
     implementation) *)
  val config : 'a t -> FileEngine.config

  (* `filename t`: return the main filename used for the file. *)
  val filename : 'a t -> string

  (* `filenames t`: returns all the filenames currently used for the
     file implementation *)
  val filenames : 'a t -> string list

  (* `length t`: returns the number of items in the file. The
     semantics depends on the type of file structure. *)
  val length : 'a t -> int

  (* Force write to disk (useful only for memory-mapped files0 *)
  val commit : ?sync:bool -> 'a t -> unit

  (* Counters are additional integers that can be stored at the beginning of files *)
  val counters : 'a t -> int

  (* `get_counter counter`: return the value of `counter` (from 0 to `counters t - 1`) *)
  val get_counter : 'a t -> int -> int

  (* `set_counter counter n`: set value of `counter` (from 0 to `counters t - 1`) to `n` *)
  val set_counter : 'a t -> int -> int -> unit

  val async : 'a t -> bool
end

(* Errors that can be raised by functions in the library *)
type error =
    BadMagic of (* Found *) string * (* Expected *) string
  | WriteToReadOnlyFile
  | OutOfBounds of string * int
  | IncompatibleVariableSizeEncoding of string
  | OperationOnClosedFile of string
  | NotImplemented of string
  | NotWorkingWithZero of string

exception Error of (* filename *) string * error

let error file_name error = raise (Error (file_name, error))

let string_of_error error =
  match error with
  | BadMagic (found, expected) -> Printf.sprintf "BadMagic { found = %S, expected = %S }" found expected
  | WriteToReadOnlyFile -> "WriteToReadOnlyFile"
  | OutOfBounds ( string , int) -> Printf.sprintf "OutOfBounds { operation = %S ; pos = %d }" string int
  | IncompatibleVariableSizeEncoding string -> Printf.sprintf "IncompatibleVariableSizeEncoding { operation = %S }" string
  | OperationOnClosedFile string -> Printf.sprintf "OperationOnClosedFile { operation = %S }" string
  | NotImplemented string -> Printf.sprintf "NotImplemented { operation = %S }" string
  | NotWorkingWithZero string -> Printf.sprintf "NotWorkingWithZero { operation = %S }" string


let catch f x =
  try
    f x
with
  Error (filename, error) ->
    Printf.eprintf "Error in file %S: %s\n%!" filename ( string_of_error error );
    exit 2


(* Only works on 64-bit systems. Changes must be made otherwise *)
let () = assert (Sys.word_size = 64)

(* Mode used to open files *)
type mode =
  | Create     (* Create the file if needed and open in read-write mode *)
  | ReadOnly   (* The file must already exist. Open in read-only mode *)
  | ReadWrite  (* The file must already exist. Open in read-write mode *)
