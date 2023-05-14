#[cfg(test)]
use anyhow::Result;
#[cfg(test)]
use thirtyfour::WebDriver;
#[cfg(test)]
mod smoke;
#[cfg(test)]
mod ui;

#[cfg(test)]
/// A helper function to creae a webdriver connection
pub(crate) async fn connect_to_webdriver() -> Result<WebDriver> {
    use std::env;
    use thirtyfour::{DesiredCapabilities, TimeoutConfiguration};

    //let caps = DesiredCapabilities::firefox();
    let caps = DesiredCapabilities::chrome();

    let host = env::var("WEBUI_TESTER_DRIVER_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("WEBUI_TESTER_DRIVER_PORT").unwrap_or_else(|_| "9515".to_string());
    let driver = WebDriver::new(format!("http://{host}:{port}").as_str(), caps).await?;

    // Keep all timeouts unchanged except for the implicit timeout (timeout of
    // when to abort locating an element). This helps because the UI renders
    // lazily with react. (e.g., without sometimes it will complain it can't
    // find a menu-item because first load of the page can be a bit slower than
    // the rest)
    let timeouts =
        TimeoutConfiguration::new(None, None, Some(std::time::Duration::from_millis(2000)));
    driver.update_timeouts(timeouts).await?;

    let host = env::var("WEBUI_TESTER_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("WEBUI_TESTER_PORT").unwrap_or_else(|_| "8080".to_string());
    driver
        .goto(format!("http://{host}:{port}").as_str())
        .await?;

    Ok(driver)
}
