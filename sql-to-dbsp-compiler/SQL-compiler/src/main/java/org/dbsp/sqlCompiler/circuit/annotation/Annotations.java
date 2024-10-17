package org.dbsp.sqlCompiler.circuit.annotation;

import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class Annotations {
    public final List<Annotation> annotations;

    public Annotations() {
        this.annotations = new ArrayList<>();
    }

    public Annotations(Annotations other) {
        this.annotations = new ArrayList<>(other.annotations);
    }

    public void replace(Annotations annotations) {
        if (!this.annotations.isEmpty())
            this.annotations.clear();
        if (!annotations.isEmpty())
            this.annotations.addAll(annotations.annotations);
    }

    public boolean isEmpty() {
        return this.annotations.isEmpty();
    }

    public void add(Annotation annotation) {
        this.annotations.add(annotation);
    }

    public boolean contains(Predicate<Annotation> test) {
        return Linq.any(this.annotations, test);
    }

    public List<Annotation> get(Predicate<Annotation> test) { return Linq.where(this.annotations, test); }

    public String toDotString() {
        List<Annotation> toShow = Linq.where(this.annotations, t -> !t.invisible());
        if (toShow.isEmpty())
            return "";
        return " " + toShow;
    }

    @Override
    public String toString() {
        return this.annotations.toString();
    }
}
