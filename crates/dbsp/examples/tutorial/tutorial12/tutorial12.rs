use anyhow::Result;
use dbsp::typed_batch::IndexedZSetReader;
use dbsp::{
    NestedCircuit, Runtime, Stream,
    utils::{Tup2, Tup3},
};
use dbsp::{OrdZSet, ZWeight, zset};

// Some helper types.
type WeightedValue<K> = Tup2<K, ZWeight>;
type String2 = Tup2<String, String>;
type String3 = Tup3<String, String, String>;

// And some helper functions.
fn owned_string2(((s1, s2), weight): ((&str, &str), ZWeight)) -> WeightedValue<String2> {
    Tup2(Tup2(s1.to_owned(), s2.to_owned()), weight)
}
fn owned_string3(((s1, s2, s3), weight): ((&str, &str, &str), ZWeight)) -> WeightedValue<String3> {
    Tup2(Tup3(s1.to_owned(), s2.to_owned(), s3.to_owned()), weight)
}

fn main() -> Result<()> {
    const STEPS: usize = 3;

    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let (
        mut circuit,
        (
            (
                alloc_input,
                assign_input,
                virtual_call_input,
                heap_type_input,
                dispatch_input,
                actual_arg_input,
                formal_param_input,
            ),
            (var_points_to_output, call_graph_output),
        ),
    ) = Runtime::init_circuit(threads, move |root_circuit| {
        let (alloc, alloc_input) = root_circuit.add_input_zset::<String2>();
        let (assign, assign_input) = root_circuit.add_input_zset::<String2>();
        let (virtual_call, virtual_call_input) = root_circuit.add_input_zset::<String3>();
        let (heap_type, heap_type_input) = root_circuit.add_input_zset::<String2>();
        let (dispatch, dispatch_input) = root_circuit.add_input_zset::<String3>();
        let (actual_arg, actual_arg_input) = root_circuit.add_input_zset::<String2>();
        let (formal_param, formal_param_input) = root_circuit.add_input_zset::<String2>();

        let (var_points_to, call_graph) = root_circuit.recursive(
            // Note that recursive has an implicit distinct (see its Docs)!
            |child_circuit,
             // var_points_to and call_graph are the recursive computations we are
             // interested in.
             (var_points_to, call_graph): (
                Stream<NestedCircuit, OrdZSet<String2>>,
                Stream<NestedCircuit, OrdZSet<String2>>,
            )| {
                // Import streams from the parent circuit into the child circuit.
                let alloc = alloc.delta0(child_circuit);
                let assign = assign.delta0(child_circuit);
                let virtual_call = virtual_call.delta0(child_circuit);
                let heap_type = heap_type.delta0(child_circuit);
                let dispatch = dispatch.delta0(child_circuit);
                let actual_arg = actual_arg.delta0(child_circuit);
                let formal_param = formal_param.delta0(child_circuit);

                // call_graph_next computes this Datalog query:
                // CallGraph(Site, Meth) :-
                //     VirtualCall(Site, Recv, Sig),
                //     VarPointsTo(Recv, Obj),
                //     HeapType(Obj, Ty),
                //     Dispatch(Ty, Sig, Meth).
                let call_graph_next = virtual_call
                    .map_index(|Tup3(site, recv, sig)| {
                        (recv.clone(), Tup3(site.clone(), recv.clone(), sig.clone()))
                    })
                    .join_index(
                        // 1. virtual_call JOIN var_points_to ON recv
                        // Mutual recursion: call_graph uses var_points_to
                        &var_points_to.map_index(|Tup2(recv, obj)| {
                            (recv.clone(), Tup2(recv.clone(), obj.clone()))
                        }),
                        |_recv, Tup3(site, _, sig), Tup2(_, obj)| {
                            Some((obj.clone(), Tup3(site.clone(), sig.clone(), obj.clone())))
                        },
                    )
                    .join_index(
                        // 2. ... JOIN heap_type ON obj
                        &heap_type.map_index(|Tup2(obj, ty)| {
                            (obj.clone(), Tup2(obj.clone(), ty.clone()))
                        }),
                        |_obj, Tup3(site, sig, _), Tup2(_, ty)| {
                            Some((
                                Tup2(ty.clone(), sig.clone()),
                                Tup2(site.clone(), ty.clone()),
                            ))
                        },
                    )
                    .join_index(
                        // 3. ... JOIN dispatch ON ty and sig
                        &dispatch.map_index(|Tup3(ty, sig, meth)| {
                            (Tup2(ty.clone(), sig.clone()), meth.clone())
                        }),
                        |_, Tup2(site, _), meth| {
                            Some((
                                Tup2(site.clone(), meth.clone()),
                                Tup2(site.clone(), meth.clone()),
                            ))
                        },
                    );

                // var_points_to_next computes this Datalog query:
                // VarPointsTo(V, Obj) :- Alloc(V, Obj).
                // VarPointsTo(Dst, Obj) :-
                //     Assign(Dst, Src),
                //     VarPointsTo(Src, Obj).
                // VarPointsTo(Param, Obj) :-
                //     CallGraph(Site, Meth),
                //     ActualArg(Site, Arg),
                //     FormalParam(Meth, Param),
                //     VarPointsTo(Arg, Obj).
                let var_points_to_next = var_points_to // Recursion: var_points_to is also self-recursive
                    .map_index(|Tup2(src, obj)| (src.clone(), Tup2(src.clone(), obj.clone())))
                    .join_index(
                        // var_points_to JOIN assign ON src
                        &assign.map_index(|Tup2(dst, src)| {
                            (src.clone(), Tup2(dst.clone(), src.clone()))
                        }),
                        |_src, Tup2(_, obj), Tup2(dst, _)| {
                            Some((
                                Tup2(dst.clone(), obj.clone()),
                                Tup2(dst.clone(), obj.clone()),
                            ))
                        },
                    )
                    .plus(
                        // "base case": alloc feeds into var_points_to
                        &alloc.map_index(|Tup2(var, obj)| {
                            (
                                Tup2(var.clone(), obj.clone()),
                                Tup2(var.clone(), obj.clone()),
                            )
                        }),
                    )
                    .plus(
                        // This argument to plus() computes this part of the Datalog query:
                        // VarPointsTo(Param, Obj) :-
                        //     CallGraph(Site, Meth),
                        //     ActualArg(Site, Arg),
                        //     FormalParam(Meth, Param),
                        //     VarPointsTo(Arg, Obj).
                        // Mutual recursion: var_points_to uses call_graph
                        &call_graph
                            .map_index(|Tup2(site, meth)| {
                                (site.clone(), Tup2(site.clone(), meth.clone()))
                            })
                            .join_index(
                                // 1. call_graph JOIN actual_arg ON site
                                &actual_arg.map_index(|Tup2(site, arg)| {
                                    (site.clone(), Tup2(site.clone(), arg.clone()))
                                }),
                                |_site, Tup2(_, meth), Tup2(_, arg)| {
                                    Some((meth.clone(), Tup2(meth.clone(), arg.clone())))
                                },
                            )
                            .join_index(
                                // .2. ... JOIN formal_param ON meth
                                &formal_param.map_index(|Tup2(meth, param)| {
                                    (meth.clone(), Tup2(meth.clone(), param.clone()))
                                }),
                                |_meth, Tup2(_, arg), Tup2(_, param)| {
                                    Some(((arg.clone()), Tup2(arg.clone(), param.clone())))
                                },
                            )
                            .join_index(
                                // 3. ... JOIN var_points_to ON arg
                                &var_points_to.map_index(|Tup2(arg, obj)| {
                                    (arg.clone(), Tup2(arg.clone(), obj.clone()))
                                }),
                                |_arg, Tup2(_, param), Tup2(_, obj)| {
                                    Some((
                                        Tup2(param.clone(), obj.clone()),
                                        Tup2(param.clone(), obj.clone()),
                                    ))
                                },
                            ),
                    );

                Ok((
                    var_points_to_next
                        .map(|(Tup2(param, obj), _)| Tup2(param.clone(), obj.clone())),
                    call_graph_next.map(|(Tup2(site, meth), _)| Tup2(site.clone(), meth.clone())),
                ))
            },
        )?;

        Ok((
            (
                alloc_input,
                assign_input,
                virtual_call_input,
                heap_type_input,
                dispatch_input,
                actual_arg_input,
                formal_param_input,
            ),
            (
                var_points_to.accumulate_output(),
                call_graph.accumulate_output(),
            ),
        ))
    })?;

    // Define the inputs at each "step".

    let mut alloc_inputs = ([
        vec![(("g", "oG"), 1), (("d", "oDog"), 1), (("c", "oCat"), 1)]
            .into_iter()
            .map(owned_string2)
            .collect(),
        vec![(("m", "oMouse"), 1)]
            .into_iter()
            .map(owned_string2)
            .collect(),
        vec![],
    ] as [Vec<WeightedValue<String2>>; STEPS])
        .into_iter();

    let mut assign_inputs = ([
        vec![(("ac", "c"), 1)]
            .into_iter()
            .map(owned_string2)
            .collect(),
        vec![],
        vec![(("ac", "c"), -1)]
            .into_iter()
            .map(owned_string2)
            .collect(),
    ] as [Vec<WeightedValue<String2>>; STEPS])
        .into_iter();

    let mut virtual_call_inputs = ([
        vec![
            (("s1", "g", "greet"), 1),
            (("s2", "g", "greet"), 1),
            (("s3", "x", "speak"), 1),
        ]
        .into_iter()
        .map(owned_string3)
        .collect(),
        vec![(("s4", "g", "greet"), 1)]
            .into_iter()
            .map(owned_string3)
            .collect(),
        vec![(("s2", "g", "greet"), -1)]
            .into_iter()
            .map(owned_string3)
            .collect(),
    ] as [Vec<WeightedValue<String3>>; STEPS])
        .into_iter();

    let mut heap_type_inputs = ([
        vec![
            (("oG", "Greeter"), 1),
            (("oDog", "Dog"), 1),
            (("oCat", "Cat"), 1),
        ]
        .into_iter()
        .map(owned_string2)
        .collect(),
        vec![(("oMouse", "Mouse"), 1)]
            .into_iter()
            .map(owned_string2)
            .collect(),
        vec![],
    ] as [Vec<WeightedValue<String2>>; STEPS])
        .into_iter();

    let mut dispatch_inputs = ([
        vec![
            (("Greeter", "greet", "Greeter.greet"), 1),
            (("Dog", "speak", "Dog.speak"), 1),
            (("Cat", "speak", "Cat.speak"), 1),
        ]
        .into_iter()
        .map(owned_string3)
        .collect(),
        vec![(("Mouse", "speak", "Mouse.speak"), 1)]
            .into_iter()
            .map(owned_string3)
            .collect(),
        vec![],
    ] as [Vec<WeightedValue<String3>>; STEPS])
        .into_iter();

    let mut actual_arg_inputs = ([
        vec![(("s1", "d"), 1), (("s2", "ac"), 1)]
            .into_iter()
            .map(owned_string2)
            .collect(),
        vec![(("s4", "m"), 1)]
            .into_iter()
            .map(owned_string2)
            .collect(),
        vec![(("s2", "ac"), 1)]
            .into_iter()
            .map(owned_string2)
            .collect(),
    ] as [Vec<WeightedValue<String2>>; STEPS])
        .into_iter();

    let mut formal_param_inputs = ([
        vec![(("Greeter.greet", "x"), 1)]
            .into_iter()
            .map(owned_string2)
            .collect(),
        vec![],
        vec![],
    ] as [Vec<WeightedValue<String2>>; STEPS])
        .into_iter();

    // Define the expected outputs at each "step".

    let mut var_points_to_expected_outputs = ([
        zset! {
            Tup2("ac".to_string(), "oCat".to_string()) => 1,
            Tup2("c".to_string(), "oCat".to_string()) => 1,
            Tup2("d".to_string(), "oDog".to_string()) => 1,
            Tup2("g".to_string(), "oG".to_string()) => 1,
            Tup2("x".to_string(), "oDog".to_string()) => 1,
            Tup2("x".to_string(), "oCat".to_string()) => 1,
        },
        zset! {
            Tup2("m".to_string(), "oMouse".to_string()) => 1,
            Tup2("x".to_string(), "oMouse".to_string()) => 1,
        },
        zset! {
            Tup2("ac".to_string(), "oCat".to_string()) => -1,
            Tup2("x".to_string(), "oCat".to_string()) => -1,
        },
    ] as [OrdZSet<String2>; STEPS])
        .into_iter();

    let mut call_graph_expected_outputs = ([
        zset! {
            Tup2("s1".to_string(), "Greeter.greet".to_string()) => 1,
            Tup2("s2".to_string(), "Greeter.greet".to_string()) => 1,
            Tup2("s3".to_string(), "Dog.speak".to_string()) => 1,
            Tup2("s3".to_string(), "Cat.speak".to_string()) => 1,
        },
        zset! {
            Tup2("s3".to_string(), "Mouse.speak".to_string()) => 1,
            Tup2("s4".to_string(), "Greeter.greet".to_string()) => 1,
        },
        zset! {
            Tup2("s2".to_string(), "Greeter.greet".to_string()) => -1,
            Tup2("s3".to_string(), "Cat.speak".to_string()) => -1,
        },
    ] as [OrdZSet<String2>; STEPS])
        .into_iter();

    // Execute the circuit.
    for i in 1..=STEPS {
        // 1. Feed input data.
        alloc_input.append(&mut alloc_inputs.next().unwrap());
        assign_input.append(&mut assign_inputs.next().unwrap());
        virtual_call_input.append(&mut virtual_call_inputs.next().unwrap());
        heap_type_input.append(&mut heap_type_inputs.next().unwrap());
        dispatch_input.append(&mut dispatch_inputs.next().unwrap());
        actual_arg_input.append(&mut actual_arg_inputs.next().unwrap());
        formal_param_input.append(&mut formal_param_inputs.next().unwrap());

        // 2. Execute the transaction.
        circuit.transaction()?;

        // 3. Print and check outputs.
        println!("=== Outputs Iteration {i} ===");
        println!("=== VarPointsTo Output ===");
        let var_points_to_output = var_points_to_output.concat();
        var_points_to_output
            .iter()
            .for_each(|(Tup2(var, o_type), _, z_weight)| {
                println!("var: {var} -> object_type: {o_type} => {z_weight}");
            });
        assert_eq!(
            var_points_to_output.consolidate(),
            var_points_to_expected_outputs.next().unwrap(),
        );
        println!("=== CallGraph Output ===");
        let call_graph_output = call_graph_output.concat();
        call_graph_output
            .iter()
            .for_each(|(Tup2(call_site, method), _, z_weight)| {
                println!("call_site {call_site} -> method {method} => {z_weight}");
            });
        assert_eq!(
            call_graph_output.consolidate(),
            call_graph_expected_outputs.next().unwrap(),
        );
    }

    Ok(())
}
