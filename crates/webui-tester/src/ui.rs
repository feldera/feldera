//! Reusable UI elements for writing WebUI tests.
#![allow(dead_code)] // disable 10 dead code warnings because our tests doesn't use everything in
                     // here yet

use anyhow::{anyhow, Result};
use thirtyfour::components::Component;
use thirtyfour::components::ElementResolver;
use thirtyfour::prelude::*;
use tokio::time::Duration;
use tokio::time::Instant;

/// A link in the main navgation on the left side of the screen.
#[derive(Debug, Clone, Component)]
pub struct VerticalMenuLink {
    /// This is the MuiListItem-root.
    base: WebElement,
    /// This is the <a /> element.
    #[by(class = "MuiButtonBase-root", first)]
    link: ElementResolver<WebElement>,
}

impl VerticalMenuLink {
    /// Name of the menu item.
    pub async fn name(&self) -> WebDriverResult<String> {
        let elem = self.link.resolve().await?;
        elem.text().await
    }

    /// Click the menu item (navigates to the page).
    pub async fn click(&self) -> WebDriverResult<()> {
        let elem = self.link.resolve().await?;
        elem.click().await?;
        Ok(())
    }
}

/// The main navigation on the left side of the screen.
#[derive(Debug, Clone, Component)]
pub struct VerticalMenu {
    /// This is the outer <ul> MuiList-root
    base: WebElement,
    /// These are the <li> elements.
    #[by(class = "MuiListItem-root", allow_empty)]
    items: ElementResolver<Vec<VerticalMenuLink>>,
}

impl VerticalMenu {
    /// Navigate to the given menu item.
    ///
    /// # Arguments
    /// - `name` - The name of the menu item to navigate to.
    pub async fn navigate_to(&self, name: &str) -> Result<()> {
        for menu_item in self.items.resolve().await? {
            if menu_item.name().await? == name {
                menu_item.click().await?;
                return Ok(());
            }
        }
        Err(anyhow!(format!("Could not find menu item '{}'", name)))
    }
}

/// Sets the text of a (hopefully form) element.
async fn set_text_generic(elem: &ElementResolver<WebElement>, text: &str) -> WebDriverResult<()> {
    let elem = elem.resolve().await?;
    elem.clear().await?;
    elem.send_keys(text).await?;
    Ok(())
}

/// Retrieves the text of an element.
async fn get_text_generic(elem: &ElementResolver<WebElement>) -> WebDriverResult<String> {
    let elem = elem.resolve().await?;
    elem.text().await
}

/// Generic function that waits until the text of `elem` is `wait_for_this`.
///
/// Aborts if `timeout` is reached.
async fn wait_until_generic(
    elem: &ElementResolver<WebElement>,
    wait_for_this: &str,
    timeout: Duration,
) -> Result<()> {
    let now = Instant::now();
    loop {
        let status = get_text_generic(elem).await?;
        if status == wait_for_this {
            break;
        } else if now.elapsed() > timeout {
            return Err(anyhow!(
                "TimoutReached: status was at '{status}' after {timeout:?}.",
            ));
        } else {
            log::debug!("Waiting for `{wait_for_this}` (currently at `{status}`)...");
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    }

    Ok(())
}

/// The editor page.
#[derive(Debug, Clone, Component)]
pub struct SqlEditor {
    base: WebElement,
    #[by(id = "program-name", first)]
    form_name: ElementResolver<WebElement>,
    #[by(id = "program-description", first)]
    form_description: ElementResolver<WebElement>,
    #[by(class = "view-line", first)]
    form_code: ElementResolver<WebElement>,
    #[by(id = "save-indicator", first)]
    save_indicator: ElementResolver<WebElement>,
    #[by(id = "compile-indicator", first)]
    compile_indicator: ElementResolver<WebElement>,
}

impl SqlEditor {
    /// Get the name of the program.
    pub async fn name(&self) -> WebDriverResult<String> {
        get_text_generic(&self.form_name).await
    }

    /// Get the description of the program.
    pub async fn description(&self) -> WebDriverResult<String> {
        get_text_generic(&self.form_description).await
    }

    /// Get the code of the program.
    pub async fn code(&self) -> WebDriverResult<String> {
        get_text_generic(&self.form_code).await
    }

    /// Set the name of the program.
    pub async fn set_name(&self, name: &str) -> WebDriverResult<()> {
        set_text_generic(&self.form_name, name).await
    }

    /// Set the description of the program.
    pub async fn set_description(&self, description: &str) -> WebDriverResult<()> {
        set_text_generic(&self.form_description, description).await
    }

    /// Set the code of the program.
    ///
    /// This needs the driver as an argument because the code editor is not a
    /// normal input field. We can't do `send_keys` on the element directly.
    pub async fn set_code(&self, code: &str, driver: &WebDriver) -> WebDriverResult<()> {
        let elem = self.form_code.resolve().await?;
        elem.click().await?;

        driver.active_element().await?;
        driver
            .action_chain()
            .click_element(&elem)
            .send_keys(code)
            .perform()
            .await?;
        Ok(())
    }

    /// Read the save status of the program.
    pub async fn save_status(&self) -> WebDriverResult<String> {
        get_text_generic(&self.save_indicator).await
    }

    /// Read the compilation status of the program.
    pub async fn compilation_status(&self) -> WebDriverResult<String> {
        get_text_generic(&self.compile_indicator).await
    }

    /// Wait until `save_status` reaches "SAVED".
    ///
    /// Return an error if it doesn't reach "SAVED" within the given timeout.
    pub async fn wait_until_saved(&self, timeout: Duration) -> Result<()> {
        wait_until_generic(&self.save_indicator, "SAVED", timeout).await
    }

    /// Wait until `compilation_status` reaches "SUCCESS".
    ///
    /// Return an error if it doesn't reach "SUCCESS" within the given timeout.
    pub async fn wait_until_compiled(&self, timeout: Duration) -> Result<()> {
        wait_until_generic(&self.compile_indicator, "SUCCESS", timeout).await
    }
}

/// The add generic connector button.
#[derive(Debug, Clone, Component)]
pub struct AddGenericConnectorButton {
    base: WebElement,
    #[by(class = "MuiButton-root", first)]
    add: ElementResolver<WebElement>,
}

/// The create connector page.
#[derive(Debug, Clone, Component)]
pub struct ConnectorCreator {
    base: WebElement,
    #[by(id = "generic-connector", first)]
    generic_connector: ElementResolver<AddGenericConnectorButton>,
}

impl ConnectorCreator {
    /// Click the "Add" button on the generic connector and create a generic
    /// connector.
    pub async fn add_generic_connector(
        &self,
        driver: &WebDriver,
        name: &str,
        description: &str,
        config: &str,
    ) -> WebDriverResult<()> {
        let elem = self.generic_connector.resolve().await?;
        elem.add.resolve().await?.click().await?;

        // Need to wait until editor initializes
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let dialog =
            AddGenericConnectorDialog::from(driver.find(By::Id("generic-connector-form")).await?);
        dialog.set_name(name).await?;
        dialog.set_description(description).await?;
        dialog.set_config(config).await?;

        dialog.create.resolve().await?.click().await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Component)]
pub struct AddGenericConnectorDialog {
    base: WebElement,
    #[by(id = "connector-name", first)]
    form_name: ElementResolver<WebElement>,
    #[by(id = "connector-description", first)]
    form_description: ElementResolver<WebElement>,
    #[by(class = "inputarea", first)]
    form_config: ElementResolver<WebElement>,
    #[by(css = "button[type='submit']", first)]
    create: ElementResolver<WebElement>,
}

impl AddGenericConnectorDialog {
    pub async fn set_name(&self, name: &str) -> WebDriverResult<()> {
        set_text_generic(&self.form_name, name).await
    }

    pub async fn set_description(&self, description: &str) -> WebDriverResult<()> {
        set_text_generic(&self.form_description, description).await
    }

    pub async fn set_config(&self, config: &str) -> WebDriverResult<()> {
        let elem = self.form_config.resolve().await?;
        elem.focus().await?;

        // Remove all pre-existing text:
        //
        // This doesn't seem to have any effect -- the editor is a weird
        // combination of divs:
        elem.clear().await?;
        // Instead we do this which works:
        for _i in 0..256 {
            // TODO: get length by reading current text
            elem.send_keys("" + &Key::Delete).await?;
        }

        tokio::time::sleep(Duration::from_millis(800)).await;

        // Insert the new text
        elem.send_keys(config).await?;

        tokio::time::sleep(Duration::from_millis(3600)).await;

        Ok(())
    }
}

/// Connect a input handle to a table.
///
/// # Notes on why this function is so complicated
///
/// - The straight forward way would be to use `drag_and_drop_element` -- but it
///   didn't work. See also https://github.com/SeleniumHQ/selenium/issues/8003 and
///   https://github.com/w3c/webdriver/issues/1488
/// - The "fix" in thirtyfour is `js_drag_to` -- didn't work.
/// - A bunch of other javascript hacks with xpath / no jquery etc. --
///   didn't work See also
///   https://stackoverflow.com/questions/71188974/how-to-perform-drag-and-drop-on-html5-element-using-selenium-3-141-59-java
///
/// What did end up working is this:
/// https://github.com/SeleniumHQ/selenium/issues/3269#issuecomment-452757044
/// e.g., put a bunch of sleeps around the actions.
pub async fn drag_and_drop_impl(
    driver: &WebDriver,
    from: &WebElement,
    to: &WebElement,
) -> Result<()> {
    // TODO: the 800ms seems high but if we go lower (e.g., 200ms) it looks
    // like the browser is still connecting things but the attachment is not
    // saved properly in the backend, unclear yet if this is a bug in the UI
    // or still the drag-and-drop bug in selenium
    driver
        .action_chain()
        .move_to_element_center(from)
        .perform()
        .await?;
    tokio::time::sleep(Duration::from_millis(800)).await;
    driver
        .action_chain()
        .click_and_hold_element(from)
        .perform()
        .await?;
    tokio::time::sleep(Duration::from_millis(800)).await;
    driver
        .action_chain()
        .move_to_element_center(to)
        .perform()
        .await?;
    tokio::time::sleep(Duration::from_millis(800)).await;
    driver
        .action_chain()
        .move_by_offset(-1, -1)
        .release()
        .perform()
        .await?;
    tokio::time::sleep(Duration::from_millis(800)).await;

    Ok(())
}

/// The pipeline builder page.
#[derive(Debug, Clone, Component)]
pub struct PipelineBuilder {
    base: WebElement,
    #[by(id = "pipeline-name", first)]
    form_name: ElementResolver<WebElement>,
    #[by(id = "pipeline-description", first)]
    form_description: ElementResolver<WebElement>,
    #[by(id = "sql-program-select", first)]
    form_program: ElementResolver<WebElement>,
    #[by(id = "save-indicator", first)]
    save_indicator: ElementResolver<WebElement>,
    #[by(xpath = "//*[@data-id='inputPlaceholder']", first)]
    input_placeholder: ElementResolver<WebElement>,
    #[by(xpath = "//*[@data-id='outputPlaceholder']", first)]
    output_placeholder: ElementResolver<WebElement>,
    #[by(css = ".react-flow__node-sqlProgram", single)]
    sql_program: ElementResolver<SqlProgram>,

    /// Input connectors.
    ///
    /// Make sure to access with `resolve_present` as they become present after
    /// creation of `Self`.
    #[by(css = ".react-flow__node-inputNode", allow_empty)]
    inputs: ElementResolver<Vec<InputNode>>,

    /// Output connectors.
    ///
    /// Make sure to access with `resolve_present` as they become present after
    /// creation of `Self`.
    #[by(css = ".react-flow__node-outputNode", allow_empty)]
    outputs: ElementResolver<Vec<OutputNode>>,
}

/// The sql program node in the builder page.
#[derive(Debug, Clone, Component)]
pub struct SqlProgram {
    base: WebElement,
    /// The draggable table handles.
    #[by(css = ".tableHandle", allow_empty)]
    tables: ElementResolver<Vec<TableHandle>>,
    /// The draggable view handles.
    #[by(xpath = "//div[contains(@data-handleid, 'view-')]", allow_empty)]
    views: ElementResolver<Vec<ViewHandle>>,
}

/// The sql input nodes in the builder page.
#[derive(Debug, Clone, Component)]
pub struct OutputNode {
    base: WebElement,
    /// The draggable handle for the input.
    #[by(css = ".outputHandle", first)]
    handle: ElementResolver<WebElement>,
}

impl OutputNode {
    /// Get the id of the attached connector.
    pub async fn name(&self) -> Result<String> {
        let handle_id = self
            .handle
            .resolve()
            .await?
            .attr("data-id")
            .await?
            .expect("property needs to exist");
        Ok(handle_id)
    }

    /// What's displayed on that connector box.
    ///
    /// It should be the connector name and description.
    pub async fn display_text(&self) -> WebDriverResult<String> {
        self.base.text().await
    }
}

/// The sql input nodes in the builder page.
#[derive(Debug, Clone, Component)]
pub struct InputNode {
    base: WebElement,
    /// The draggable handle for the input.
    #[by(css = ".inputHandle", first)]
    handle: ElementResolver<WebElement>,
}

impl InputNode {
    /// Get the id of the attached connector.
    pub async fn name(&self) -> Result<String> {
        let handle_id = self
            .handle
            .resolve()
            .await?
            .attr("data-id")
            .await?
            .expect("property needs to exist");
        Ok(handle_id)
    }

    /// What's displayed on that connector box.
    ///
    /// It should be the connector name and description.
    pub async fn display_text(&self) -> WebDriverResult<String> {
        self.base.text().await
    }

    /// Connect a input handle to a table.
    pub async fn connect_with(&self, driver: &WebDriver, table: &TableHandle) -> Result<()> {
        let handle = self.handle.resolve_present().await?;
        drag_and_drop_impl(driver, &handle, &table.base).await
    }
}

/// The Table inside of the SqlProgram node.
#[derive(Debug, Clone, Component)]
pub struct TableHandle {
    base: WebElement,
}

impl TableHandle {
    /// Get the name of the table.
    pub async fn name(&self) -> Result<String> {
        let handle_id = self
            .base
            .attr("data-handleid")
            .await?
            .expect("property needs to exist");
        Ok(handle_id.split('-').nth(1).unwrap().to_string())
    }
}

/// The View inside of the SqlProgram node.
#[derive(Debug, Clone, Component)]
pub struct ViewHandle {
    base: WebElement,
}

impl ViewHandle {
    /// Get the name of the view.
    pub async fn name(&self) -> Result<String> {
        let handle_id = self
            .base
            .attr("data-handleid")
            .await?
            .expect("property needs to exist");
        Ok(handle_id.split('-').nth(1).unwrap().to_string())
    }

    /// Connect a input handle to a table.
    pub async fn connect_with(&self, driver: &WebDriver, output: &OutputNode) -> Result<()> {
        let output_handle = output.handle.resolve_present().await?;
        drag_and_drop_impl(driver, &self.base, &output_handle).await
    }
}

impl PipelineBuilder {
    /// Get the name of the program.
    pub async fn name(&self) -> WebDriverResult<String> {
        get_text_generic(&self.form_name).await
    }

    /// Get the description of the program.
    pub async fn description(&self) -> WebDriverResult<String> {
        get_text_generic(&self.form_description).await
    }

    /// Set the name of the program.
    pub async fn set_name(&self, name: &str) -> WebDriverResult<()> {
        set_text_generic(&self.form_name, name).await
    }

    /// Set the description of the program.
    pub async fn set_description(&self, description: &str) -> WebDriverResult<()> {
        set_text_generic(&self.form_description, description).await
    }

    /// Set the program.
    pub async fn set_program(&self, program: &str) -> WebDriverResult<()> {
        set_text_generic(&self.form_program, program).await
    }

    /// Read the save status of the program.
    pub async fn save_status(&self) -> WebDriverResult<String> {
        get_text_generic(&self.save_indicator).await
    }

    /// Wait until `save_status` reaches "SAVED".
    ///
    /// Return an error if it doesn't reach "SAVED" within the given timeout.
    pub async fn wait_until_saved(&self, timeout: Duration) -> Result<()> {
        wait_until_generic(&self.save_indicator, "SAVED", timeout).await
    }

    pub async fn find_connector_in_drawer(
        &self,
        driver: &WebDriver,
        connector: &str,
    ) -> Result<()> {
        // A drawer should've opened
        let drawer = ConnectorDrawer::from(driver.find(By::Id("connector-drawer")).await?);

        // Going through all potential connector lists to find the first named
        // `connector` We can't just iterate over buttons because the whole
        // drawer becomes invalid when we click on a select button.
        let buttons_len = drawer.select_buttons.resolve_present().await?.len();
        for idx in 0..buttons_len {
            let button = &drawer.select_buttons.resolve_present().await?[idx];
            button.click().await?;
            // This keeps the ID of the drawer but we changed the content, don't
            // reference stuff in drawer at the moment as most of it is gone and
            // will throw an runtime error in selenium
            let table = PickConnectorTable::from(driver.find(By::Id("connector-drawer")).await?);
            if table.try_add(connector).await? {
                break;
            } else {
                table.back().await?;
                // Stuff in `drawer` is valid again, aren't we lucky?
            }
        }

        Ok(())
    }

    /// Add an output connector to the pipeline.
    pub async fn add_output(&self, driver: &WebDriver, connector: &str) -> Result<()> {
        let oph = self.output_placeholder.resolve().await?;
        oph.click().await?;
        self.find_connector_in_drawer(driver, connector).await
    }

    /// Add an input connector to the pipeline.
    pub async fn add_input(&self, driver: &WebDriver, connector: &str) -> Result<()> {
        let iph = self.input_placeholder.resolve().await?;
        iph.click().await?;
        self.find_connector_in_drawer(driver, connector).await
    }

    /// Connect the input connector to the given table.
    ///
    /// # TODO
    /// For building more complicated tests, we might need to return the
    /// attached connector name in `add_input` and use that instead of connector
    /// name.
    pub async fn connect_output(
        &self,
        driver: &WebDriver,
        view_name: &str,
        connector_name: &str,
    ) -> Result<()> {
        let inputs = self.outputs.resolve_present().await?;

        // We find the first input that has the given connector name. See todo
        // for why we can do this better in the future.
        let mut output_handle = None;
        for input in inputs.iter() {
            if input.display_text().await?.contains(connector_name) {
                output_handle = Some(input);
                break;
            }
        }

        if let Some(output_handle) = output_handle {
            let program_node = self.sql_program.resolve_present().await?;
            let views = program_node.views.resolve_present().await?;
            for view in views.iter() {
                if view.name().await? == view_name {
                    view.connect_with(driver, output_handle).await?;
                    return Ok(());
                }
            }
            Err(anyhow!("`view` not found in program views"))
        } else {
            Err(anyhow!("`connector_name` not found in outputs"))
        }
    }

    /// Connect the input connector to the given table.
    ///
    /// # TODO
    /// For building more complicated tests, we might need to return the
    /// attached connector name in `add_input` and use that instead of connector
    /// name.
    pub async fn connect_input(
        &self,
        driver: &WebDriver,
        connector_name: &str,
        table_name: &str,
    ) -> Result<()> {
        let inputs = self.inputs.resolve_present().await?;

        // We find the first input that has the given connector name. See todo
        // for why we can do this better in the future.
        let mut input_handle = None;
        for input in inputs.iter() {
            if input.display_text().await?.contains(connector_name) {
                input_handle = Some(input);
                break;
            }
        }

        if let Some(input_handle) = input_handle {
            let program_node = self.sql_program.resolve_present().await?;
            let tables = program_node.tables.resolve_present().await?;
            for table in tables.iter() {
                if table.name().await? == table_name {
                    input_handle.connect_with(driver, table).await?;
                    return Ok(());
                }
            }
            Err(anyhow!("`table` not found in program tables"))
        } else {
            Err(anyhow!("`connector_name` not found in inputs"))
        }
    }
}

/// The connector drawer to pick existing connectors.
#[derive(Debug, Clone, Component)]
pub struct ConnectorDrawer {
    base: WebElement,
    /// All the buttons which are used to select a connector.
    #[by(
        xpath = "//button[contains(text(),'Select')][not(@disabled)]",
        allow_empty
    )]
    select_buttons: ElementResolver<Vec<WebElement>>,
}

/// The connector table (in the drawer) which lists existing connectors to pick
/// from.
#[derive(Debug, Clone, Component)]
pub struct PickConnectorTable {
    base: WebElement,
    /// Link back to the connector types.
    #[by(xpath = "//a[contains(text(),'Add Input Source')]", first)]
    back: ElementResolver<WebElement>,
    /// The name columns in the table.
    ///
    /// The column header also gets the data-field='name' attribute, so we need
    /// to filter that out.
    #[by(
        xpath = "//div[@data-field='name' and @role!='columnheader']",
        allow_empty
    )]
    names: ElementResolver<Vec<WebElement>>,
    /// The add button on each row.
    #[by(xpath = "//div[@data-field='actions']/button", allow_empty)]
    add: ElementResolver<Vec<WebElement>>,
}

impl PickConnectorTable {
    /// Click the back button.
    pub async fn back(&self) -> WebDriverResult<()> {
        let elem = self.back.resolve().await?;
        elem.click().await?;
        Ok(())
    }

    /// Find the first row with the given name and click the add button.
    ///
    /// Return `true` if the row was found and the button was clicked, `false`
    /// otherwise.
    pub async fn try_add(&self, connector: &str) -> Result<bool> {
        assert_eq!(
            self.names.resolve().await?.len(),
            self.add.resolve().await?.len(),
            "Something is wrong with the xpath selectors"
        );

        for (i, elem) in self.names.resolve().await?.iter().enumerate() {
            if elem.text().await? == connector {
                let add = self.add.resolve().await?;
                add[i].click().await?;
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// The pipeline table where one can start/stop existing pipelines.
#[derive(Debug, Clone, Component)]
pub struct PipelineManagementTable {
    /// The table.
    base: WebElement,
    /// The rows in the table.
    #[by(class = "MuiDataGrid-row", allow_empty)]
    rows: ElementResolver<Vec<PipelineRow>>,
}

impl PipelineManagementTable {
    /// Find the row index for config with `name`
    async fn find_row(&self, name: &str) -> Result<usize> {
        let rows = self.rows.resolve().await?;
        for (idx, elem) in rows.iter().enumerate() {
            if elem.name.resolve().await?.text().await? == name {
                return Ok(idx);
            }
        }

        Err(anyhow!("`name` not found in table"))
    }

    /// Open the detailed view of the row (by clicking the plus button)
    pub async fn open_details(&self, driver: &WebDriver, name: &str) -> Result<PipelineDetails> {
        let row_idx = self.find_row(name).await?;
        let row = &self.rows.resolve().await?[row_idx];
        let details_button = row.details_button.resolve().await?;
        details_button.click().await?;

        let details = PipelineDetails::from(
            driver
                .find(By::ClassName("MuiDataGrid-detailPanel"))
                .await?,
        );
        Ok(details)
    }

    /// Find the pipeline identified with `name` and press start.
    pub async fn start(&self, name: &str) -> Result<()> {
        let row_idx = self.find_row(name).await?;
        let start = self.rows.resolve().await?[row_idx].start.resolve().await?;
        start.click().await?;
        Ok(())
    }

    /// Find the pipeline identified with `name` and press pause.
    pub async fn pause(&self, name: &str) -> Result<()> {
        let row_idx = self.find_row(name).await?;
        let pause = self.rows.resolve().await?[row_idx]
            .pause
            .resolve_present()
            .await?;
        pause.click().await?;
        Ok(())
    }

    /// Find the pipeline identified with `name` and press shutdown.
    pub async fn shutdown(&self, name: &str) -> Result<()> {
        let row_idx = self.find_row(name).await?;
        let shutdown = self.rows.resolve().await?[row_idx]
            .shutdown
            .resolve()
            .await?;
        shutdown.click().await?;
        Ok(())
    }

    /// Find the pipeline identified with `name` and press shutdown.
    pub async fn edit(&self, name: &str) -> Result<()> {
        let row_idx = self.find_row(name).await?;
        let edit = self.rows.resolve().await?[row_idx].edit.resolve().await?;
        edit.click().await?;
        Ok(())
    }

    /// Wait until pipeline status reaches `RUNNING`.
    ///
    /// Return an error if it doesn't reach `RUNNING` within the given timeout.
    pub async fn wait_until_running(&self, name: &str, timeout: Duration) -> Result<()> {
        let row_idx = self.find_row(name).await?;
        let row = &self.rows.resolve().await?[row_idx];
        wait_until_generic(&row.status, "RUNNING", timeout).await
    }
}

/// A row in the  pipeline table.
#[derive(Debug, Clone, Component)]
struct PipelineRow {
    /// The row.
    base: WebElement,
    /// The names in the table.
    #[by(css = "[data-field='name']", single)]
    name: ElementResolver<WebElement>,
    /// The status field of all the rows.
    #[by(css = "[data-field='status']", single)]
    status: ElementResolver<WebElement>,
    /// The details button of a rows.
    #[by(css = "[data-testid='AddIcon']", first)]
    details_button: ElementResolver<WebElement>,
    /// The start button of a row.
    #[by(class = "startButton", first)]
    start: ElementResolver<WebElement>,
    /// The pause button of a row.
    #[by(class = "pauseButton", first)]
    pause: ElementResolver<WebElement>,
    /// The edit button of a row.
    #[by(class = "editButton", first)]
    edit: ElementResolver<WebElement>,
    /// The shutdown button of a row.
    #[by(class = "shutdownButton", first)]
    shutdown: ElementResolver<WebElement>,
}

impl PipelineRow {
    pub async fn status(&self) -> WebDriverResult<String> {
        let elem = self.status.resolve().await?;
        elem.text().await
    }
}

/// The details view that opens when you expand a pipeline row.
#[derive(Debug, Clone, Component)]
pub struct PipelineDetails {
    base: WebElement,
    #[by(class = "inputStats", first)]
    input_stats: ElementResolver<InputStats>,
    #[by(class = "outputStats", first)]
    output_stats: ElementResolver<OutputStats>,
}

impl PipelineDetails {
    /// Returns the number of records the input has received.
    pub async fn get_records_for_table(&self, table_name: &str) -> Result<usize> {
        let input_stats = self.input_stats.resolve().await?;
        let tables = input_stats.tables.resolve().await?;
        for (idx, table) in tables.iter().enumerate() {
            if table.text().await? == table_name {
                let record_str = input_stats.records.resolve().await?[idx].text().await?;
                return Ok(record_str.parse::<usize>()?);
            }
        }
        Err(anyhow!("`table` not found in input stats"))
    }

    /// Returns the number of records the output has produced.
    pub async fn get_records_for_view(&self, view_name: &str) -> Result<usize> {
        let output_stats = self.output_stats.resolve().await?;
        let views = output_stats.views.resolve().await?;
        for (idx, view) in views.iter().enumerate() {
            if view.text().await? == view_name {
                let record_str = output_stats.records.resolve().await?[idx].text().await?;
                return Ok(record_str.parse::<usize>()?);
            }
        }
        Err(anyhow!("`view` not found in output stats"))
    }
}

/// Input stats table.
#[derive(Debug, Clone, Component)]
struct InputStats {
    base: WebElement,
    #[by(css = "[data-field='config']", allow_empty)]
    tables: ElementResolver<Vec<WebElement>>,
    #[by(css = "[data-field='records']", allow_empty)]
    records: ElementResolver<Vec<WebElement>>,
}

/// Output stats table.
#[derive(Debug, Clone, Component)]
struct OutputStats {
    base: WebElement,
    #[by(css = "[data-field='config']", allow_empty)]
    views: ElementResolver<Vec<WebElement>>,
    #[by(css = "[data-field='records']", allow_empty)]
    records: ElementResolver<Vec<WebElement>>,
}
