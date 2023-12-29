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

open EzMapfile

let verbosity = ref 0

let openfile_config = FileEngine.openfile_config
let mapfile_config = FileEngine.mapfile_config ~mapsize:(Some 2_000_000_000L)

let random_string n =
  String.init n (fun _ -> char_of_int (Random.int 256) )

let test1 () =
  let open Mapfile in
  let list = ref [] in
  begin
    let t = openfile
        ~mapsize:40_000_000_000L
        "test-mapfile.db"
        ~create:true
        ~readonly:false
    in

    let pos = ref 0 in
    for len = 0 to 10000 do

      let s = random_string len in
      let pos1 = !pos in
      write_fixed_string t ~off:pos1 s ~pos:0 ~len ;
      let pos2 = pos1 + len in
      let pos3 = write_string t ~off:pos2 s ~pos:0 ~len in
      pos := pos3;
      list := (s, pos1, pos2, len) :: !list
    done;

    List.iter (fun (s,pos1, pos2, len) ->

        let b = read_fixed t ~off:pos1 ~len:len in
        assert (Bigstring.to_string b = s);

        let b = read t ~off:pos2 in
        assert (Bigstring.to_string b = s);
      ) !list;

    commit t ~sync:true;
    close t;
  end;

  begin
    let t = openfile
        ~mapsize:40_000_000_000L
        "test-mapfile.db"
        ~readonly:true
        ~create:false
    in
    List.iter (fun (s,pos1, pos2, len) ->

        let b = read_fixed t ~off:pos1 ~len:len in
        assert (Bigstring.to_string b = s);

        let b = read t ~off:pos2 in
        assert (Bigstring.to_string b = s);

        let sz = read_size t ~off:pos2 in
        assert (sz = len);
      ) !list;
    close t;
  end;

  ()

type 'a data_type = {
  name : string ;
  encoding : 'a FileEncoding.t ;
  random : ( unit -> 'a ) ;
  to_string : ('a -> string);
  zero : 'a ;
}

let random_string ?size () =
  let n = match size with
    | None -> Random.int 50
    | Some n -> n
  in
  let b = Bytes.create n in
  if n > 2 then begin
    Bytes.set b 0 (char_of_int 255);
    for i = 1 to n-2 do
      Bytes.set b i ( char_of_int ( 65 + Random.int 26 ) )
    done;
    Bytes.set b (n-1) (char_of_int 0);
  end else begin
    for i = 0 to n-1 do
      Bytes.set b i ( char_of_int ( 65 + Random.int 26 ) )
    done;
  end;
  Bytes.unsafe_to_string b

let string_type =
  let name = "string" in
  let encoding = FileEncoding.string "STR" in
  let random () = random_string () in
  let to_string s = s in
  let zero = "" in
  {
    name ;
    encoding ;
    random ;
    to_string ;
    zero ;
  }

let string5_type =
  let name = "string5" in
  let encoding = FileEncoding.string ~size:5 "ST5" in
  let random () = random_string ~size:5 () in
  let to_string s = s in
  let zero = "" in
  {
    name ;
    encoding ;
    random ;
    to_string ;
    zero ;
  }

let int_type ?(max=1_000_000_000) ~name encoding =
  let random () = Random.full_int max - max/2 in
  let to_string = string_of_int in
  let zero = 0 in
  {
    name; encoding ; random ; to_string ; zero ;
  }

let int8_type =
  let name = "int8" in
  let encoding = FileEncoding.int8 "i8" in
  int_type ~max:256 ~name encoding

let int16na_type =
  let name = "int16na" in
  let encoding = FileEncoding.int16na "i16" in
  int_type ~max:65536 ~name encoding

let int16no_type =
  let name = "int16no" in
  let encoding = FileEncoding.int32na "i16" in
  int_type ~max:65536 ~name encoding

let int32na_type =
  let name = "int32na" in
  let encoding = FileEncoding.int32na "i32" in
  int_type ~max:(1 lsl 32) ~name encoding

let int32no_type =
  let name = "int32no" in
  let encoding = FileEncoding.int32no "i32" in
  int_type ~max:(1 lsl 32) ~name encoding


let int63na_type =
  let name = "int63na" in
  let encoding = FileEncoding.int63na "i63" in
  int_type ~max:(1 lsl 59) ~name encoding

let int63no_type =
  let name = "int63no" in
  let encoding = FileEncoding.int63no "i63" in
  int_type ~max:(1 lsl 59) ~name encoding


let verbose ?(n=1) fmt =
  if !verbosity >= n then
    Printf.eprintf fmt
  else
    Printf.ifprintf stderr fmt

let verbose0 fmt = verbose ~n:0 fmt
let verbose1 = verbose
let verbose2 fmt = verbose ~n:2 fmt

let test_sequence ~n ~has_zero config value_type () =

  let encoding = value_type.encoding in
  let first_name = Printf.sprintf "sequence-%s-first" value_type.name in
  let second_name = Printf.sprintf "sequence-%s-second" value_type.name in
  let third_name = Printf.sprintf "sequence-%s-third" value_type.name in

  let sequence = Array.init n (fun _i -> value_type.random () )in

  let check file positions =
    verbose "  Read Sequence\n%!";
    Array.iteri (fun i pos ->
        verbose "Read %d at %d\n%!" i pos;
        let v = FileSequence.get file pos in
        let s = sequence.(i) in
        if v <> s then begin
          verbose0 "Difference at index %d position %d:\n%!" i pos ;
          verbose0 "  In memory: %S\n%!" (value_type.to_string s);
          verbose0 "  On disk: %S\n%!"   (value_type.to_string v);
          exit 2
        end;

      ) positions;
  in

  let zero = if has_zero then
      Some value_type.zero
    else
      None
  in
  let positions =
    let file = FileSequence.create Create ?zero ~config encoding first_name in

    verbose "  Write Sequence\n%!";
    let positions =
      Array.mapi (fun i v ->
          verbose "    Write %S at index %d\n%!" (value_type.to_string v) i;
          let pos = FileSequence.add file v in
          verbose "      returned %d\n%!" pos;
          let end_pos = FileSequence.end_pos file in
          verbose "      end_pos %d\n%!" end_pos;
          let s = FileSequence.get file pos in
          if v <> s then begin
            verbose0 "Difference at index %d position %d:\n%!" i pos ;
            verbose0 "  In memory: %S\n%!" (value_type.to_string v);
            verbose0 "  On disk: %S\n%!"   (value_type.to_string s);
            exit 2
          end;
          pos
        ) sequence in

    check file positions;
    verbose "  Rename Sequence\n%!";
    FileSequence.rename file second_name;

    verbose "  Close Sequence\n%!";
    FileSequence.close file;
    positions
  in

  verbose "  Rename file\n%!";
  Sys.rename second_name third_name ;

  begin
    verbose "  Reopen Sequence\n%!";
    let file = FileSequence.create ReadWrite ?zero ~config encoding third_name in
    verbose "  Close Sequence\n%!";
    FileSequence.close file;
  end;

  begin
    verbose "  Reopen Sequence\n%!";
    let file = FileSequence.create ReadOnly ?zero ~config encoding third_name in


    check file positions;

    verbose "  Close Sequence\n%!";
    FileSequence.close file;
    verbose "  Delete Sequence\n%!";
    FileSequence.delete file
  end;

  assert ( not @@ Sys.file_exists third_name )


let test_array ~n ~has_zero config value_type () =

  let encoding = value_type.encoding in
  let first_name = Printf.sprintf "array-%s-first" value_type.name in
  let second_name = Printf.sprintf "array-%s-second" value_type.name in
  let third_name = Printf.sprintf "array-%s-third" value_type.name in

  let array = Array.init n (fun _i -> value_type.random () )in

  let zero = if has_zero then
      Some value_type.zero
    else
      None
  in

  let check file =
    verbose "  Read Array\n%!";
    Array.iteri (fun i s ->
        verbose "Read %d\n%!" i;
        let v = FileArray.get file i in
        if v <> s then begin
          verbose0 "Difference at index %d\n%!" i ;
          verbose0 "  In memory: %S\n%!" (value_type.to_string s);
          verbose0 "  On disk: %S\n%!"   (value_type.to_string v);
          exit 2
        end;

      ) array;
  in

  begin
    let file = FileArray.create Create ?zero ~config encoding first_name in

    verbose "  Write Array\n%!";
    Array.iteri (fun i v ->
        verbose "    Write %S at index %d\n%!" (value_type.to_string v) i;
        let pos = FileArray.add file v in
        verbose "      returned %d\n%!" pos;
        assert ( pos = i );
      ) array ;

    check file;

    verbose "  Rename Array to second\n%!";
    FileArray.rename file second_name;

    verbose "  Close Array\n%!";
    FileArray.close file;
  end;

  verbose "  Rename file to third\n%!";
  Sys.rename second_name third_name ;
  if Sys.file_exists ( second_name ^ ".index" ) then
    Sys.rename ( second_name ^ ".index" ) ( third_name ^ ".index" ) ;

  begin
    verbose "  Reopen Array\n%!";
    let file = FileArray.create ReadWrite ?zero ~config encoding third_name in

    FileArray.compress file;
    verbose "  Close Array\n%!";
    FileArray.close file;
  end;

  begin
    verbose "  Reopen Array\n%!";
    let file = FileArray.create ReadOnly ?zero ~config encoding third_name in

    check file;

    verbose "  Close Array\n%!";
    FileArray.close file;
    verbose "  Delete Array\n%!";
    FileArray.delete file
  end;

  assert ( not @@ Sys.file_exists third_name )

let concatmap f list = String.concat " . " ( List.map f list )

let test_hashtbl ~n config key_type value_type () =

  let prefix = Printf.sprintf "hashtbl-%s-%s" key_type.name value_type.name in
  let first_name = prefix ^ "-first" in
  let second_name = prefix ^ "-second" in
  let third_name = prefix ^ "-third" in

  let initial_size = 17 in

  let h = Hashtbl.create initial_size in
  let array = Array.init n (fun _i ->
      let key = key_type.random () in
      let value = value_type.random () in
      Hashtbl.add h key value;
      (key, value)
    ) in


  let check file =
    verbose "  Read Hashtbl\n%!";
    Array.iteri (fun i (k,v) ->
        verbose "Read %d for key %S and value %S\n%!"
          i (key_type.to_string k) (value_type.to_string v);

        let v_good = Hashtbl.find h k in
        let v_all_good = Hashtbl.find_all h k in

        let must_exit = ref false in
        verbose "  find\n%!";
        let v_found = FileHashtbl.find_opt file k in

        begin
          match v_found with
          | None ->
              verbose0 "Value not found !!!\n%!"
          | Some v_found ->
              if v_found <> v_good then begin
                verbose0 "Difference:\n%!" ;
                verbose0 "  In memory: %S\n%!" (value_type.to_string v_good);
                verbose0 "  On disk: %S\n%!"   (value_type.to_string v_found);
                must_exit := true;
              end;
        end;

        verbose "  find_all\n%!";
        let v_all_found = FileHashtbl.find_all file k in

        if v_all_found <> v_all_good then begin
          verbose0 "Difference:\n%!" ;
          verbose0 "  In memory: %S\n%!"
            (concatmap value_type.to_string v_all_good);
          verbose0 "  On disk: %S\n%!"
            (concatmap value_type.to_string v_all_found);
          must_exit := true;
        end;

        if !must_exit then exit 2
      ) array;
  in

  begin
    let file = FileHashtbl.create Create ~config
        ~key_enc:key_type.encoding
        ~value_enc:value_type.encoding ~initial_size first_name in

    verbose "  Write Hashtbl\n%!";

    Array.iteri (fun i (k,v) ->
        verbose "    %d: Write %S at key %S\n%!"
          i
          (value_type.to_string v)
          (key_type.to_string k);
        FileHashtbl.add file k v ;
        verbose "      done\n%!";
        begin match FileHashtbl.find_opt file k with
          | None ->
              verbose0 "    not stored !!\n%!";
              exit 2
          | Some v' ->
              if v <> v' then begin
                verbose0 "    badly stored !!\n%!"
              end
        end;

        verbose "   length";
        assert ( FileHashtbl.length file = i+1);
      ) array ;

    check file;

    verbose "  Rename Hashtbl to second\n%!";
    FileHashtbl.rename file second_name;

    verbose "  Close Hashtbl\n%!";
    FileHashtbl.close file;
  end;

  verbose "  Rename file to third\n%!";
  Sys.rename second_name third_name ;
  if Sys.file_exists ( second_name ^ ".entries" ) then
    Sys.rename ( second_name ^ ".entries" ) ( third_name ^ ".entries" ) ;
  if Sys.file_exists ( second_name ^ ".collisions" ) then
    Sys.rename ( second_name ^ ".collisions" ) ( third_name ^ ".collisions" ) ;

  begin
    verbose "  Reopen Hashtbl\n%!";
    let file = FileHashtbl.create ReadWrite ~config
        ~key_enc:key_type.encoding
        ~value_enc:value_type.encoding
        third_name in

    for _ = 1 to n / 4 do

      let i = Random.int n in
      let j = Random.int n in
      let (k',v') = array.(i) in
      if i<>j && Random.int 3 = 0 then begin
        Hashtbl.remove h k' ;
        FileHashtbl.remove file k' ;
      end;
      let (k,_v) = array.(j) in
      array.(i) <- (k, v');
      Hashtbl.add h k v';
      FileHashtbl.add file k v';
    done;

    check file;

    let new_size = FileHashtbl.length file / 2 in
    FileHashtbl.resize ~clean:true file ~new_size;

    verbose "  Close Hashtbl\n%!";
    FileHashtbl.close file;
  end;

  begin
    verbose "  Reopen Hashtbl\n%!";
    let file = FileHashtbl.create ReadOnly ~config
        ~key_enc:key_type.encoding
        ~value_enc:value_type.encoding
        third_name
    in

    check file ;

    verbose "  Close Hashtbl\n%!";
    FileHashtbl.close file;
    verbose "  Delete Hashtbl\n%!";
    FileHashtbl.delete file
  end;

  assert ( not @@ Sys.file_exists third_name )


let flatmap f list = List.flatten (List.map f list)
let configs = [ "openfile", openfile_config ; "mapfile", mapfile_config ]

let string_encodings =
  [
    string_type ;
    string5_type
  ]
let int_encodings = [
  int8_type ;
  int16na_type ;
  int16no_type ;
  int32na_type ;
  int32no_type ;
  int63na_type ;
  int63no_type ;
]

let build_sequence_tests encodings =
  flatmap
    (fun (config_name, config) ->
       flatmap
         (fun (size_name, n) ->
            flatmap
              (fun (zero_name, has_zero) ->
                 List.map (fun data_type ->
                     let g () = test_sequence ~n ~has_zero config data_type () in
                     g,
                     [ "sequence" ; data_type.name ; zero_name ; config_name ; size_name ]
                   ) encodings
              ) [ "zero", true ; "no-zero", false ]
         ) ["min", 1000 ; "average", 10_000; "max", 1_000_000 ]
    ) configs

let build_array_tests encodings =
  flatmap
    (fun (config_name, config) ->
       flatmap
         (fun (size_name, n) ->
            flatmap
              (fun (zero_name, has_zero) ->
                 List.map (fun data_type ->
                     let g () = test_array ~n ~has_zero config data_type () in
                     g,
                     [ "array" ; data_type.name ; zero_name ; config_name ; size_name ]
                   ) encodings
              ) [ "zero", true ; "no-zero", false ]
         ) ["min", 1000 ; "average", 10_000; "max", 1_000_000 ]
    ) configs

let build_hashtbl_tests encodings1 encodings2 =
  flatmap
    (fun (config_name, config) ->
       flatmap
         (fun (size_name, n) ->
              flatmap (fun data_type1 ->
                  List.map (fun data_type2 ->
                      let g () = test_hashtbl ~n config data_type1 data_type2 () in
                      g,
                      [ "hashtbl" ; data_type1.name ; data_type2.name ; config_name ; size_name ]
                    ) encodings2
                ) encodings1
         ) ["min", 1000 ; "average", 10_000; "max", 100_000 ]
    ) configs

let tests = [
  test1, [ "mapfile" ];
]
  @ build_sequence_tests string_encodings
  @ build_sequence_tests int_encodings

  @ build_array_tests string_encodings
  @ build_array_tests int_encodings

  @ build_hashtbl_tests string_encodings string_encodings
  @ build_hashtbl_tests string_encodings int_encodings
  @ build_hashtbl_tests int_encodings string_encodings
  @ build_hashtbl_tests int_encodings int_encodings


let test ~list ?(keywords=[]) ?name () =
  let n = ref 0 in
  List.iter (fun (test_fun, test_keywords) ->
      let test_name = String.concat "." test_keywords in
      if
        match keywords, name with
        | _ :: _, Some name ->
            Printf.kprintf failwith "Cannot provide both -k %S and %S"
              ( String.concat "." keywords ) name
        | _, Some name -> test_name = name
        | [], None -> true
        | keywords, None -> List.for_all (fun k -> List.mem k test_keywords) keywords
      then begin
        incr n;
        if list then
          Printf.printf "Test %S\n%!" test_name
        else begin
          Printf.printf "Starting test %S\n%!" test_name;
          Random.init (1 lsl 30 -1 );
          let t0 = Unix.gettimeofday () in
          Types.catch test_fun ();
          let t1 = Unix.gettimeofday () in
          Printf.printf "Test %S successful (%.2fs)\n%!" test_name (t1 -. t0);
        end
      end
    ) tests ;
  if list then
    Printf.printf "%d listed ran\n%!" !n
  else
    Printf.printf "%d tests ran\n%!" !n


let () =
  ignore ( Sys.command "rm -rf _tests" = 0);
  ignore ( Sys.command "mkdir _tests" = 0);
  Unix.chdir "_tests";
  let keywords = ref [] in
  let names = ref [] in
  let list = ref false in
  Arg.parse
    [
      "-l", Arg.Set list, " Only list tests";
      "--list", Arg.Set list, " Only list tests";
      "-k",
      Arg.String (fun s ->
          keywords := ( String.lowercase_ascii s ) :: !keywords
        ), "KEYWORD Run all test with keyword" ;
      "-v", Arg.Unit (fun () -> incr verbosity), " Increase verbosity";
    ]
    (fun s -> names := String.lowercase_ascii s :: !names )
    "ez-mapfile-test [OPTIONS]* [TEST-NAME]*";

  let list = !list in
  begin match !keywords, !names with
    | [], [] -> test ~list ()
    | [], names ->
        List.iter (fun name ->
            test ~list ~name ()
          ) (List.rev names) ;
    | keywords, [] ->
        test ~list ~keywords ()
    | _ -> failwith "You cannot specify both names and keywords"
  end
