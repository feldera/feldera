// REST API pages: collapse each nested object's fields behind a "Show N child attributes"
// toggle, as Stripe does. The plugin renders a nested object as a table inside the field's cell.
// React owns those tables, so we do not move them: we add a button before each one and hide the
// table with a class.

const BUTTON_CLASS = "api-child-toggle";
const HIDDEN_CLASS = "api-child-hidden";

function label(open: boolean, count: number): string {
  const noun = count === 1 ? "child attribute" : "child attributes";
  return `${open ? "Hide" : "Show"} ${count} ${noun}`;
}

function collapseChildAttributes(): void {
  const root = document.querySelector(".theme-api-markdown");
  if (!root) return;
  root.querySelectorAll<HTMLTableElement>("td > table").forEach((table) => {
    const cell = table.parentElement as HTMLTableCellElement;
    // Only a field's children: the cell starts with the field name. The top-level "Schema"
    // table under a response code stays open.
    if (!cell.querySelector(":scope > code")) return;
    if (table.previousElementSibling?.classList.contains(BUTTON_CLASS)) return;

    const count = table.querySelectorAll(":scope > tbody > tr").length;
    const button = document.createElement("button");
    button.type = "button";
    button.className = BUTTON_CLASS;
    button.setAttribute("aria-expanded", "false");
    button.textContent = label(false, count);
    button.addEventListener("click", () => {
      const open = table.classList.toggle(HIDDEN_CLASS) === false;
      button.setAttribute("aria-expanded", String(open));
      button.textContent = label(open, count);
    });
    table.classList.add(HIDDEN_CLASS);
    cell.insertBefore(button, table);
  });
}

// 224 of the 225 error responses in the spec share one schema (ErrorResponse). Each run of them
// becomes one "Errors" card, as on Stripe: one row per status code and one link to the error
// object, instead of the same three fields under every code.
const ERROR_FIELDS = "details,error_code,message";
const ERROR_DOCS = "/api/introduction#feldera-error-response-body";

// What each status code means in Feldera, for responses the spec leaves undescribed.
// Wording follows "Feldera client errors" and "Feldera server errors" on the introduction page.
const STATUS_DEFAULTS: Record<string, string> = {
  "400": "The request is not valid.",
  "401": "The request has no valid API key.",
  "403": "The API key does not give access to this resource.",
  "404": "A resource that the request names does not exist.",
  "405": "The method is not supported for this resource.",
  "409": "The request conflicts with the current state of a resource.",
  "410": "The resource is no longer available.",
  "500": "The server could not process the request because of an unexpected failure.",
  "501": "The server does not implement this feature, for example in the open-source edition.",
  "503": "The server cannot process the request yet. Try again later.",
  "504": "The server did not get a response in time.",
};

function isSharedError(head: HTMLElement): boolean {
  const table = head.nextElementSibling?.querySelector("table");
  if (!table) return false;
  const fields = [...table.querySelectorAll(":scope > tbody > tr > td > code:first-child")]
    .map((c) => c.textContent?.trim())
    .sort()
    .join(",");
  return fields === ERROR_FIELDS;
}

// Mark the top-level sections as cards, and group the shared error responses.
function cardSections(): void {
  const root = document.querySelector(".theme-api-markdown");
  if (!root) return;
  root.querySelectorAll<HTMLTableElement>(":scope > table").forEach((table) => {
    if (table.classList.contains("api-section")) return;
    table.classList.add("api-section");
    const title = table.querySelector(":scope > thead th")?.textContent?.trim();
    if (title !== "Responses") return;
    table.classList.add("api-responses");

    const rows = [...table.querySelectorAll<HTMLTableRowElement>(":scope > tbody > tr")];
    rows.forEach((row) => {
      const head = row.querySelector<HTMLElement>(':scope > td > div[style*="flex"]:first-child');
      const status = head?.querySelector("code")?.textContent?.trim() ?? "";
      if (!head || !/^[45]\d\d$/.test(status)) return;

      // A sane default when the spec gives no description.
      const desc = head.children[1] as HTMLElement | undefined;
      if (desc && !desc.textContent?.trim() && STATUS_DEFAULTS[status]) {
        const span = document.createElement("span");
        span.className = "api-status-default";
        span.textContent = STATUS_DEFAULTS[status];
        desc.appendChild(span);
      }

      if (!isSharedError(head)) return;
      (head.nextElementSibling as HTMLElement).classList.add(HIDDEN_CLASS);
      row.classList.add("api-error");
    });

    // Each run of shared errors is one card with one header.
    rows.forEach((row, i) => {
      if (!row.classList.contains("api-error")) return;
      const prev = rows[i - 1]?.classList.contains("api-error");
      const next = rows[i + 1]?.classList.contains("api-error");
      if (!prev) {
        row.classList.add("api-error-first");
        const header = document.createElement("div");
        header.className = "api-errors-head";
        header.innerHTML = `<span>Errors</span><a href="${ERROR_DOCS}">Each returns the error object →</a>`;
        row.firstElementChild?.insertBefore(header, row.firstElementChild.firstChild);
      }
      if (!next) row.classList.add("api-error-last");
    });
  });
}

// Field heads for the two-column layout: the name with its type and flag under it, in the left
// column. The plugin writes "name type — OPTIONAL" as loose siblings, so we build a new head from
// their text and hide the originals (React keeps its own nodes).
function buildFieldHeads(): void {
  const root = document.querySelector(".theme-api-markdown");
  if (!root) return;
  root.querySelectorAll<HTMLTableCellElement>("td").forEach((cell) => {
    const name = cell.firstElementChild;
    if (name?.tagName !== "CODE" || cell.classList.contains("api-field")) return;

    const parts: HTMLElement[] = [];
    let type = "";
    let flag = "";
    for (let el = name.nextElementSibling; el && /^(SPAN|STRONG)$/.test(el.tagName); el = el.nextElementSibling) {
      const text = el.textContent?.replace("—", "").trim() ?? "";
      if (el.tagName === "STRONG") flag = text.toLowerCase();
      else if (text) type = text;
      parts.push(el as HTMLElement);
    }

    const head = document.createElement("div");
    head.className = "api-field-head";
    // Long snake_case names may break after an underscore, never mid-word.
    const nameCopy = document.createElement("code");
    (name.textContent ?? "").split(/(?<=_)/).forEach((piece, i) => {
      if (i) nameCopy.appendChild(document.createElement("wbr"));
      nameCopy.appendChild(document.createTextNode(piece));
    });
    head.appendChild(nameCopy);
    const meta = document.createElement("span");
    meta.className = "api-field-meta";
    meta.textContent = type;
    if (flag) {
      const f = document.createElement("span");
      f.className = `api-field-flag api-field-flag--${flag}`;
      f.textContent = (type ? " · " : "") + flag;
      meta.appendChild(f);
    }
    if (type || flag) head.appendChild(meta);

    [name as HTMLElement, ...parts].forEach((el) => el.classList.add(HIDDEN_CLASS));
    cell.classList.add("api-field");
    cell.insertBefore(head, cell.firstChild);
  });
}

// On API pages the content can render after the browser looks for the URL's #anchor, so the
// page stays at the top. Scroll to it once it exists (retry for about a second).
function scrollToHash(tries = 20): void {
  const id = decodeURIComponent(window.location.hash.slice(1));
  if (!id) return;
  const target = document.getElementById(id);
  if (target) target.scrollIntoView();
  else if (tries > 0) window.setTimeout(() => scrollToHash(tries - 1), 50);
}

export function onRouteDidUpdate(): void {
  // Run after React has committed the new page.
  window.requestAnimationFrame(() => {
    cardSections();
    collapseChildAttributes();
    buildFieldHeads();
    if (window.location.pathname.startsWith("/api/")) scrollToHash();
  });
}
