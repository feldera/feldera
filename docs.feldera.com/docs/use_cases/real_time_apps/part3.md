# A browser based UI

This article is the third part of our series on building a billion cell spreadsheet in three parts:

1. **Part 1:** [Implement core spreadsheet logic using Feldera SQL](./part1.mdx).
2. **Part 2:** [Expose pipeline data to clients via a server](./part2.md).
3. **Part 3 (this article):** Add a browser-based UI.

![spreadsheet-architecture-parts](./spreadsheet-architecture-parts.svg)

- The code is available in the [GitHub repo](https://github.com/feldera/techdemo-spreadsheet).

## Introduction

We built a minimal UI for our spreadsheet using [egui](https://www.egui.rs), a Rust-based UI framework that compiles to WebAssembly and runs in the browser.

Given our spreadsheet can span a billion cells, we don’t want to load everything into memory at once. Instead, we need lazy data fetching as users scroll. The `egui_extras` [Table component](https://docs.rs/egui_extras/latest/egui_extras/struct.Table.html) only renders visible rows, which helps us seamlessly load data on demand.

:::info
The code for this article is in the `client` folder of our [GitHub repository](https://github.com/feldera/techdemo-spreadsheet). Clone the repo and follow the README instructions to run and deploy the demo.
:::

The entire UI is managed by a single [update](
https://github.com/feldera/techdemo-spreadsheet/blob/5abfb3aedc1ffa38b23341df2ed951726aef86f0/client/src/app.rs#L106) function. During runtime, egui calls this update function many times per second to determine what needs rendering.

In this article, we’ll focus on how we retrieve and cache spreadsheet cells (i.e., the client-side handling of the `GET /api/spreadsheet` endpoint discussed in part 2).

## Loading cell content

We connect to the /api/spreadsheet endpoint [as soon as the UI loads](https://github.com/feldera/techdemo-spreadsheet/blob/5abfb3aedc1ffa38b23341df2ed951726aef86f0/client/src/app.rs#L78):

```rust
let (ws_sender, ws_receiver) = {
    let egui_ctx = cc.egui_ctx.clone();
    let wakeup = move || egui_ctx.request_repaint();
    let url = format!("{}/api/spreadsheet", server);
    ewebsock::connect_with_wakeup(&url, Default::default(), wakeup).unwrap()
};
let loader = Arc::new(Loader::new(ws_sender));
```

We also configure the ewebsock library to call our wakeup closure whenever a new message arrives, triggering a re-render by invoking the [update function](
https://github.com/feldera/techdemo-spreadsheet/blob/5abfb3aedc1ffa38b23341df2ed951726aef86f0/client/src/app.rs#L106). Because of that, we can handle newly received or updated cells right at the start of `update`:

```rust
while let Some(event) = self.ws_receiver.try_recv() {
    match event {
        WsEvent::Message(WsMessage::Text(update)) => {
            let parsed = serde_json::from_str::<Cell>(&update);
            match parsed {
                Ok(cell) => {
                    self.cell_cache.set(cell.id, cell.into());
                }
                Err(e) => {
                    trace!("error parsing cell update: {:?} {:?}", update, e);
                }
            }
        }
        WsEvent::Opened => {
            self.loader.is_open.store(true, Ordering::Relaxed);
            self.loader.fetch(0..2600);
        }
        WsEvent::Closed => {
            self.loader.is_open.store(false, Ordering::Relaxed);
        }
        _ => {
            error!("unexpected event: {:?}", event);
        }
    }
}
```

- On receiving a message from `/api/spreadsheet`, the code parses it as a `Cell` and stores it in the `cell_cache`.
- When the connection first opens (`WsEvent::Opened`), we request the first 2600 cells by sending `{ from: 0, to: 2600 }` over the WebSocket.

Looking at the [implementation of cell_cache](https://github.com/feldera/techdemo-spreadsheet/blob/5abfb3aedc1ffa38b23341df2ed951726aef86f0/client/src/cell_cache.rs#L218), you’ll see it uses an `LruCache` to store cell data (evicting the oldest entries when capacity is reached). Here’s the implementation of `CellCache::set`:

```rust
pub fn set(&mut self, id: u64, c: CellContent) {
    let mut cells = self.cells.lock();
    cells.push(id, Rc::new(c));
}
```

## Rendering the cells of the spreadsheet

We use the `TableBuilder` from egui to create and render our spreadsheet table and handle mouse and keyboard events through closures provided to the API. The key [lines for retrieving and rendering cell contents](https://github.com/feldera/techdemo-spreadsheet/blob/5abfb3aedc1ffa38b23341df2ed951726aef86f0/client/src/app.rs#L358) from the CellCache are found here:

```rust
let cell = self.cell_cache.get(id);
// ...
let cell_response = cell.ui(ui);
```

where `Cell::ui` works as follows:

```rust
pub fn ui(&self, ui: &mut Ui) -> Response {
    if self.is_editing() {
        let mut content = self.write_buffer.write();
        ui.add(TextEdit::singleline(&mut *content))
    } else {
        let content = self.content.read().to_string();
        ui.add(Label::new(&content).sense(Sense::click()))
    }
}
```

- If the cell is being edited, it shows a text box containing the `write_buffer`, initialized from `raw_value` in Feldera’s `spreadsheet_view`.
- Otherwise, it displays the `computed_value`.
-
Earlier we saw that we fetch only the first 2600 cells when the connection opens. To load additional cells as the user scrolls, we use lazy loading in `CellCache::get`:

```rust
pub fn get(&mut self, id: u64) -> Rc<CellContent> {
    let mut cells = self.cells.lock();

    if let Some(c) = cells.get(&id) {
        c.clone()
    } else {
        let c = Rc::new(CellContent::empty(id));
        cells.push(id, c.clone());

        if let Some(current_range) = &self.current_range {
            if current_range.contains(&id) {
                // Already fetching this range...
                return c;
            }
        }

        let start = id.saturating_sub(self.prefetch_before_after_id);
        let end = id.saturating_add(self.prefetch_before_after_id);
        let current_range = start..end;
        self.current_range = Some(current_range.clone());
        let fetcher = self.fetcher.clone();

        let debouncer_clone = self.debouncer.clone();
        debouncer_clone
            .borrow_mut()
            .debounce(Duration::from_millis(100), move || {
                let mut max_retry = 10;
                while !fetcher.fetch(current_range.clone()) && max_retry > 0 {
                    max_retry -= 1;
                }
            });

        c
    }
}
```

- This function returns cached `CellContent` if available.
- If the cell is missing in the cache, it returns an empty cell and triggers a `{ from: start, to: end }` WebSocket request for the missing range.
- A small timeout debounces these requests, preventing excessive calls if a user scrolls quickly.

## Conclusion

In this article, we showed how to build a minimal UI for our real-time spreadsheet using egui and WebAssembly, focusing on on-demand data fetching and caching. With these techniques in place, we can efficiently handle billions of cells without overwhelming the client’s memory or bandwidth. Check out our [GitHub repository](https://github.com/feldera/techdemo-spreadsheet) for the full code and feel free to [try the live demo](https://xls.feldera.io) to see everything in action.
