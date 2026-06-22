# Tutorial 12

This tutorial deals with static program analysis. Specifically, we aim to
find out two things about a program in some fictional language using
polymorphism:

1. To which class type a variable can point to throughout the execution of the
   program (later called `VarPointsTo`).
2. Which exact method is invoked at every call site in the presence of
   polymorphism (later called `CallGraph`).

Look at the following program in our fictional language. We have a super class
`Animal` and two subclasses `Cat` and `Dog`. There is also the `Greeter` class
whose `greet()` method expects a value of type `Animal` upon which it calls
the `speak()` method (this is the polymorphic part):

```
class Greeter { void greet(Animal x) { x.speak(); } }  // s3
class Animal { void speak() {} }
class Dog  extends Animal { void speak() {...} }
class Cat  extends Animal { void speak() {...} }

void main() {
  Greeter g = new Greeter();
  Dog     d = new Dog();
  g.greet(d);                                          // s1
  Cat     c = new Cat();
  Animal ac = c;
  g.greet(ac);                                         // s2
}
```

For instance, we may want to find out which types the variable `x` in
`Greeter.greet` actually assumes (`VarPointsTo`) and to which (concrete) methods
the call site `x.speak()` resolves to (`CallGraph`).

To succinctly express `VarPointsTo` and `CallGraph` we resort to Datalog.
A fictional driver program preparing the static analysis could generate the
following base facts about the program being analyzed:

1. `Alloc(var, obj)` whenever `var = new Obj()` (new allocation).
2. `Assign(dst, src)` whenever `dst = src` (new assignment).
3. `VirtualCall(site, recv, sig)` whenever `recv.sig(...)` at call site `site`
   (imagine the driver program keeping track of all call sites as indicated
   above in the comments: `s1`, `s2`, and `s3`).
4. `HeapType(obj, ty)` stores the runtime type `ty` of an allocated
   object `obj`.
5. `Dispatch(ty, sig, meth)` stores for each type `ty` and each
   signature `sig` its method `meth`.
6. `ActualArg(site, arg)` stores for each call site `site` the given
   argument `arg`.
7. `FormalParam(meth, param)` stores for each method `meth` its
   parameter `param`.

The properties of our interest, `VarPointsTo` and `CallGraph`, can be expressed
with Datalog as follows:

```dl
// The base case is an allocation.
VarPointsTo(V, Obj) :- Alloc(V, Obj).
// The self-recursive case is an alias.
VarPointsTo(Dst, Obj) :-
    Assign(Dst, Src),
    VarPointsTo(Src, Obj).
// This resolves to which object a parameter points to. Notice that this is
// self-recursive and mutually recursive with `CallGraph`!
VarPointsTo(Param, Obj) :-
    CallGraph(Site, Meth),
    ActualArg(Site, Arg),
    FormalParam(Meth, Param),
    VarPointsTo(Arg, Obj).

// This resolves to which concrete method each call site resolves to.
// Notice that this is mutually recursive with `VarPointsTo`.
CallGraph(Site, Meth) :-
    VirtualCall(Site, Recv, Sig),
    VarPointsTo(Recv, Obj),
    HeapType(Obj, Ty),
    Dispatch(Ty, Sig, Meth).
```

[Souffle](https://souffle-lang.github.io) is a Datalog engine that can actually
run this query. The folder `data_step_1` contains the base facts to the
fictional program shown above. This is how you can run it:

```sh
souffle program_analysis.dl --fact-dir data_step_1
```

Notice how the analysis tells us, among other things, that the variable
`x` can point to both an instance of `Dog` and `Cat` (as part of
`VarPointsTo`) and that call site `s3` (corresponding to `x.speak()`)
uses both `Dog.speak` and `Cat.speak` (see `CallGraph`).

Now the fictional program is modified, say, by its programmer, to introduce
another subclass called `Mouse` of `Animal`. Also `main()` is extended by two
lines to create a mouse and have it be greeted by the greeter:

```
class Greeter { void greet(Animal x) { x.speak(); } }  // s3
class Animal { void speak() {} }
class Dog  extends Animal { void speak() {...} }
class Cat  extends Animal { void speak() {...} }
class Mouse extends Animal { void speak() {...} }

void main() {
  Greeter g = new Greeter();   // oG
  Dog     d = new Dog();       // oDog
  g.greet(d);                  // s1
  Cat     c = new Cat();       // oCat
  Animal ac = c;               // alias (exercises the Assign rule)
  g.greet(ac);                 // s2
  Mouse   m = new Mouse()      // oMouse
  g.greet(m);                  // s4
}
```

The new base facts _and_ the old ones are stored in the `data_step_2` folder.
Again you can run this via:

```sh
souffle program_analysis.dl --fact-dir data_step_2
```

This now tells us that both `x` and `m` can point to variables of type `Mouse`
and that call site `s3` (`x.speak()`) is also invoking `Mouse.speak`,
plus that call site `s4` is only using `Greeter.greet`.

Finally, we face a deletion of code. The alias `ac` is gone and, consequently,
its usage afterwards is removed, too:

```
class Greeter { void greet(Animal x) { x.speak(); } }   // s3
class Animal { void speak() {} }
class Dog  extends Animal { void speak() {...} }
class Cat  extends Animal { void speak() {...} }
class Mouse extends Animal { void speak() {...} }

void main() {
  Greeter g = new Greeter();   // oG
  Dog     d = new Dog();       // oDog
  g.greet(d);                  // s1
  Cat     c = new Cat();       // oCat
  Mouse   m = new Mouse()      // oMouse
  g.greet(m);                  // s4
}
```

As you might be guessing already, some previously derived facts are not part
of the output anymore. You can verify which ones by running:

```sh
souffle program_analysis.dl --fact-dir data_step_3
```

While each computation caused by a code change starts from scratch every time
if we rely on Souffle, we now turn towards incremental computations with DBSP.
The file `tutorial12.rs` contains the analogous DBSP circuit for the computation
of `VarPointsTo` and `CallGraph`. Note that the translation from Datalog to a
DBSP circuit has been done manually here. To observe how DBSP computes the
output _changes_ in response to the program modifications discussed above
(as opposed to a full recomputation of the state with Souffle), run:

```sh
cargo run --example tutorial12
```
