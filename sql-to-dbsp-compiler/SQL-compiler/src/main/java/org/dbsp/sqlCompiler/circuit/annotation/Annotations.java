package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class Annotations {
    public final List<Annotation> annotations;

    public Annotations() {
        this.annotations = new ArrayList<>();
    }

    public static Annotations fromJson(JsonNode annotations) {
        Annotations result = new Annotations();
        try {
            var it = annotations.elements();
            while (it.hasNext()) {
                JsonNode annotation = it.next();
                Annotation anno = Annotation.fromJson(annotation);
                result.add(anno);
            }
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        return result;
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

    /** Return the single annotation of the specified type, or null if the annotation is not found. */
    public @Nullable <T extends Annotation> T first(Class<T> clazz) {
        for (Annotation annotation : this.annotations) {
            if (annotation.is(clazz))
                return annotation.to(clazz);
        }
        return null;
    }

    /** Generate a string for displaying the annotations in a .dot graphviz file */
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

    public void asJson(JsonStream stream) {
        stream.beginArray();
        for (Annotation annotation: this.annotations)
            annotation.asJson(stream);
        stream.endArray();
    }

    public void add(Annotations annotations) {
        this.annotations.addAll(annotations.annotations);
    }
}
