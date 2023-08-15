// A basic UI workflow test.
//
// This test will:
// 1. Create a program.
// 2. Create a connector.
// 3. Create a pipeline.
// 4. Start the pipeline.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use thirtyfour::prelude::*;
use tokio::time::Duration;

use crate::ui::*;

/// This is the SQL code that will be saved.
///
/// I noticed that if you put newlines immediately after the braces, the code
/// editor might add another brace to complete it, and then the code won't be
/// correct anymore.
const SQL_CODE: &str = "
CREATE TABLE USERS ( name varchar );
CREATE VIEW OUTPUT_USERS as SELECT * from USERS;
";

/// A config for a HTTP endpoint.
///
/// The \x08 is a backspace character, which is needed to remove the space that
/// is inserted by the editor.
const HTTP_ENDPOINT_CONFIG: &str = "
transport:
  name: http
\x08format:
  name: csv
";

#[tokio::test]
async fn smoke_workflow() -> Result<()> {
    let driver = crate::connect_to_webdriver().await?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Can't get time")
        .subsec_nanos();

    let elem = driver.find(By::Id("navigation")).await?;
    let menu = VerticalMenu::from(elem);

    // First we make a program, then we compile it.
    menu.navigate_to("SQL Programs").await?;
    driver
        .find(By::Id("btn-add-sql-program"))
        .await?
        .click()
        .await?;
    let code_page = SqlEditor::from(driver.find(By::Id("editor-content")).await?);
    let program_name = format!("My Program {}", nanos);
    code_page.set_code(SQL_CODE, &driver).await?;
    code_page.set_name(&program_name).await?;
    code_page.set_description("This is a program").await?;
    code_page.wait_until_saved(Duration::from_secs(20)).await?;
    code_page
        .wait_until_compiled(Duration::from_secs(20))
        .await?;

    // Next we make a connector.
    menu.navigate_to("Connectors").await?;
    let connector_creator =
        ConnectorCreator::from(driver.find(By::Id("connector-creator-content")).await?);
    connector_creator
        .add_generic_connector(
            &driver,
            "My HTTP Connector",
            "This is a HTTP connector",
            HTTP_ENDPOINT_CONFIG,
        )
        .await?;

    // Finally we make a pipeline.
    menu.navigate_to("Pipelines").await?;
    driver
        .find(By::Id("btn-add-pipeline"))
        .await?
        .click()
        .await?;
    let pipeline_builder =
        PipelineBuilder::from(driver.find(By::Id("pipeline-builder-content")).await?);
    let pipeline_name = format!("My Pipeline {}", nanos);
    pipeline_builder.set_name(&pipeline_name).await?;
    pipeline_builder
        .set_description("This is a pipeline")
        .await?;
    pipeline_builder.set_program(&program_name).await?;
    pipeline_builder
        .add_input(&driver, "My HTTP Connector")
        .await?;
    pipeline_builder
        .add_output(&driver, "My HTTP Connector")
        .await?;
    pipeline_builder
        .connect_input(&driver, "My HTTP Connector", "USERS")
        .await?;
    pipeline_builder
        .connect_output(&driver, "OUTPUT_USERS", "My HTTP Connector")
        .await?;
    pipeline_builder
        .wait_until_saved(Duration::from_secs(3))
        .await?;

    // Start the pipeline.
    menu.navigate_to("Pipelines").await?;
    let pipeline_management =
        PipelineManagementTable::from(driver.find(By::Id("pipeline-management-content")).await?);
    pipeline_management.start(&pipeline_name).await?;
    pipeline_management
        .wait_until_running(&pipeline_name, Duration::from_secs(10))
        .await?;

    let details = pipeline_management
        .open_details(&driver, &pipeline_name)
        .await?;
    assert_eq!(details.get_records_for_table("USERS").await?, 0);
    assert_eq!(details.get_records_for_view("OUTPUT_USERS").await?, 0);
    // TODO: send some stuff

    pipeline_management.pause(&pipeline_name).await?;

    driver.quit().await?;

    Ok(())
}
