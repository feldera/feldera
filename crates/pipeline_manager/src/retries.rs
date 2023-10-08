use std::{future::Future, time::Duration};

pub async fn retry_async<T, E, O, F: FnMut() -> O>(
    mut f: F,
    retries: i32,
    interval: Duration,
) -> Result<T, E>
where
    O: Future<Output = Result<T, E>>,
{
    let mut count = 0;
    loop {
        let result = f().await;

        if result.is_ok() {
            break result;
        } else {
            if count > retries {
                break result;
            }
            tokio::time::sleep(interval).await;
            count += 1;
        }
    }
}
