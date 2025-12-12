import { shadeOfRed } from "./util";

/** Data to display in a table cell */
export class HierarchicalTableCellValue {
    constructor(
        /** Text to display */
        readonly text: string,
        /** Title to display (tooltip) */
        readonly title: string,
        /** Importance is a number between 0 and 100. */
        readonly importance: number,
        readonly style: Partial<CSSStyleDeclaration>
    ) { }
}

/** A Row in a table */
export class HierarchicalTableRow {
    constructor(
        /** Row name */
        readonly header: string,
        /** Cells to display in the row.
         * A row is expected to have either only 1 cell, or as many cells as all the other rows */
        readonly cells: Array<HierarchicalTableCellValue>,
        /** True if this is the "selected" row. */
        readonly selected: boolean) { }
}

/** A section in a table. */
class Section {
    /** All the rows of the section */
    readonly rows: Array<HierarchicalTableRow>;

    /** Create a section with a specified name */
    constructor(readonly name: string) {
        this.rows = [];
    }
}

class VisibleSections {
    private readonly visible: Set<string>;

    constructor() {
        this.visible = new Set();
    }

    setVisibility(section: string, visible: boolean): VisibleSections {
        if (visible) {
            this.visible.add(section);
        } else {
            this.visible.delete(section);
        }
        return this;
    }

    isVisible(section: string): boolean {
        // Empty section is always visible
        if (section === "") { return true; }
        return this.visible.has(section);
    }
}

// start with all sections visible by default
const GLOBALLY_VISIBLE = new VisibleSections()
    .setVisibility("CPU", true)
    .setVisibility("memory", true)
    .setVisibility("storage", true)
    .setVisibility("cache", true);

/** Abstraction for a table-like class which has several sections which can be individually
 * collapsed.   The rows within each section are sorted.  The table "remembers" which sections
 * are collapsed. */
export class HierarchicalTable {
    readonly sections: Map<string, Section>;

    constructor(
        readonly name: string,
        readonly columnNames: Array<string>,
        // May be shared between multiple tables
        readonly collapsed: VisibleSections
    ) {
        this.sections = new Map();
    }

    /** Creates a Hierarchical table that shares the visible sections with all
     * other Hierarchical tables.  The assumption is that only one of these tables
     * is visible at any time: toggling the visibility of sections will affect
     * future displayed tables, but only the currently displayed table. */
    public static create(columnNames: Array<string>): HierarchicalTable {
        return new HierarchicalTable("", columnNames, GLOBALLY_VISIBLE);
    }

    addRow(section: string, row: HierarchicalTableRow) {
        if (this.sections.has(section)) {
            let sec = this.sections.get(section)!;
            sec.rows.push(row);
        } else {
            let sec = new Section(section);
            sec.rows.push(row);
            this.sections.set(section, sec);
        }
    }

    collapse(section: string, collapsed: boolean) {
        if (this.sections.has(section)) {
            this.collapsed.setVisibility(section, collapsed);
        }
    }

    asHtml(): HTMLElement {
        const table = document.createElement('table');

        // Header row
        const thead = table.createTHead();
        if (this.name.length != 0) {
            const row = thead.insertRow();
            const cell = row.insertCell();
            cell.colSpan = this.columnNames.length + 1;
            cell.innerText = this.name;
        }

        const headerRow = thead.insertRow();
        headerRow.insertCell(); // Empty corner cell
        for (const column of this.columnNames) {
            const th = headerRow.insertCell();
            th.textContent = column;
        }

        let tbody = table.createTBody();

        let secs = [... this.sections.keys()];
        secs.sort();
        // Keep empty section name last
        if (secs.length != 0 && secs[0] === "") {
            let first = secs.shift()!;
            secs.push(first);
        }
        for (const secName of secs) {
            let section = this.sections.get(secName)!;
            if (section.rows.length == 0) { continue; }
            let rows = [...section.rows];
            rows.sort((a, b) => a.header.localeCompare(b.header));
            let visible = this.collapsed.isVisible(secName);

            let row = tbody.insertRow();
            let rowName = row.insertCell();
            rowName.colSpan = this.columnNames.length + 1;
            rowName.innerText = this.sectionName(secName, visible);
            if (secName != null) {
                rowName.title = "Click to expand/collapse";
            }

            let sectionRows = [];
            for (const row of section.rows) {
                const tr = tbody.insertRow();
                sectionRows.push(tr);
                HierarchicalTable.setVisible(tr, visible);
                const cell = tr.insertCell();
                cell.textContent = row.header;
                if (row.selected) {
                    cell.style.backgroundColor = 'blue';
                }

                let single = row.cells.length === 1;
                for (const cell of row.cells) {
                    const td = tr.insertCell();
                    const color = shadeOfRed(cell.importance);
                    td.style.backgroundColor = color;
                    if (cell.style !== null) {
                        Object.assign(td.style, cell.style);
                    }
                    td.style.color = 'black';
                    td.textContent = cell.text;
                    td.title = cell.title;
                    if (single) {
                        td.colSpan = this.columnNames.length;
                    }
                }
            }

            if (secName != "") {
                row.onclick = () => this.toggleVisibility(secName, rowName, sectionRows);
            }
        }

        return table;
    }

    sectionName(name: string, visible: boolean): string {
        if (name === "") { return ""; }
        return (visible ? "⮝ " : "⮟ ") + name;
    }

    static setVisible(tr: HTMLTableRowElement, visible: boolean) {
        if (visible) {
            tr.style.display = ""
        } else {
            tr.style.display = "none";
        }
    }

    toggleVisibility(section: string, sectionTitle: HTMLTableCellElement, sectionRows: Array<HTMLTableRowElement>) {
        let visible = this.collapsed.isVisible(section);
        visible = !visible;
        this.collapsed.setVisibility(section, visible);
        sectionTitle.innerText = this.sectionName(section, visible);
        for (const row of sectionRows) {
            HierarchicalTable.setVisible(row, visible);
        }
        this.collapse(section, visible);
    }
}