package org.dbsp.sqlCompiler.compiler;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** The Feldera documentation site: builds the links that compiler messages cite and
 * resolves them against the Markdown sources of the site. */
public class Documentation {
    /** Root of the published site */
    public static final String SITE = "https://docs.feldera.com/";
    /** Markdown sources of the site, relative to the SQL-compiler directory */
    public static final String SOURCES = "../../docs.feldera.com/docs";

    /** A page of the site, or a heading of a page.
     *
     * @param page    Path of the page below {@link #SITE}, such as {@code sql/comparisons}.
     * @param anchor  Anchor of the heading within the page, or null for the whole page. */
    public record Link(String page, @Nullable String anchor) {
        public Link(String page) {
            this(page, null);
        }

        public String url() {
            String url = SITE + this.page;
            if (this.anchor != null)
                url += "#" + this.anchor;
            return url;
        }

        /** The line that a compiler message appends to cite the documentation */
        public String citation() {
            return "See " + this.url();
        }

        /** The Markdown source of the page: a {@code .md} file, or a {@code .mdx} file
         * when that is the one that exists */
        public Path source() {
            Path markdown = Paths.get(SOURCES, this.page + ".md");
            Path mdx = Paths.get(SOURCES, this.page + ".mdx");
            return Files.exists(mdx) && !Files.exists(markdown) ? mdx : markdown;
        }

        /** The link that a URL of the site denotes, or null for any other string */
        @Nullable
        public static Link parse(String url) {
            if (!url.startsWith(SITE))
                return null;
            String path = url.substring(SITE.length());
            int hash = path.indexOf('#');
            if (hash < 0)
                return new Link(path);
            return new Link(path.substring(0, hash), path.substring(hash + 1));
        }
    }

    /** Slugify a Markdown heading the way Docusaurus (github-slugger) does:
     * lowercase, drop punctuation, replace spaces with hyphens.
     * Underscores and hyphens are preserved. */
    public static String slugify(String heading) {
        String slug = heading.trim().toLowerCase(Locale.ENGLISH);
        slug = slug.replaceAll("[^a-z0-9 _-]", "");
        return slug.trim().replaceAll("\\s+", "-");
    }

    static final Pattern EXPLICIT_HEADING_ANCHOR = Pattern.compile("\\{#([^}]+)}\\s*$");
    static final Pattern ANCHOR_TAG = Pattern.compile("<a id=\"([^\"]+)\">");

    /** The anchors that Markdown contents define: a heading's explicit {@code {#id}}
     * or otherwise its slug, and every {@code <a id="...">} tag.  A link resolves
     * only against a full anchor; a substring match breaks in the rendered site. */
    public static Set<String> anchors(String markdown) {
        Set<String> anchors = new HashSet<>();
        for (String line : markdown.split("\n")) {
            if (line.startsWith("#")) {
                String heading = line.replaceFirst("^#+", "");
                Matcher explicit = EXPLICIT_HEADING_ANCHOR.matcher(heading);
                if (explicit.find())
                    anchors.add(explicit.group(1));
                else
                    anchors.add(slugify(heading));
            }
            Matcher tag = ANCHOR_TAG.matcher(line);
            while (tag.find())
                anchors.add(tag.group(1));
        }
        return anchors;
    }

    /** Throws if the source of the page does not exist or does not define the anchor */
    public static void checkResolves(Link link) throws IOException {
        Path source = link.source();
        if (!Files.exists(source))
            throw new RuntimeException("Documentation page not found: " + source.normalize());
        if (link.anchor() == null)
            return;
        Set<String> anchors = anchors(Files.readString(source));
        if (!anchors.contains(link.anchor()))
            throw new RuntimeException("Anchor `" + link.anchor() + "` does not appear in file " +
                    source.normalize() + "; the anchors are " + anchors);
    }
}
