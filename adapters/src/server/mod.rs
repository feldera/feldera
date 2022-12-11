use crate::{Catalog, Controller, ControllerConfig, ControllerError};
use actix_files as fs;
use actix_files::NamedFile;
use actix_web::{
    dev::{ServiceFactory, ServiceRequest},
    get,
    middleware::Logger,
    rt, web,
    web::Data as WebData,
    App, Error as ActixError, HttpResponse, HttpServer, Responder, Result as ActixResult,
};
use anyhow::Result as AnyResult;
use dbsp::DBSPHandle;
use log::error;
use std::sync::Mutex;

// TODO:
//
// - grafana

struct ServerState {
    controller: Mutex<Option<Controller>>,
}

impl ServerState {
    fn new(controller: Controller) -> Self {
        Self {
            controller: Mutex::new(Some(controller)),
        }
    }
}

pub fn run(circuit: DBSPHandle, catalog: Catalog, yaml_config: &str, port: u16) -> AnyResult<()> {
    let config: ControllerConfig = serde_yaml::from_str(yaml_config)?;
    let controller = Controller::with_config(
        circuit,
        catalog,
        &config,
        Box::new(|e| error!("{e}")) as Box<dyn Fn(ControllerError) + Send + Sync>,
    )?;

    let state = WebData::new(ServerState::new(controller));

    rt::System::new().block_on(
        HttpServer::new(move || build_app(App::new().wrap(Logger::default()), state.clone()))
            .bind(("127.0.0.1", port))?
            .run(),
    )?;

    Ok(())
}

fn build_app<T>(app: App<T>, state: WebData<ServerState>) -> App<T>
where
    T: ServiceFactory<ServiceRequest, Config = (), Error = ActixError, InitError = ()>,
{
    app.app_data(state)
        .route("/", web::get().to(index))
        .route("/index.html", web::get().to(index))
        .service(fs::Files::new("/static", "static").show_files_listing())
        .service(start)
        .service(pause)
        .service(shutdown)
        .service(status)
}

async fn index() -> ActixResult<NamedFile> {
    Ok(NamedFile::open("static/index.html")?)
}

#[get("/start")]
async fn start(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            controller.start();
            HttpResponse::Ok().body("The pipeline is running")
        }
        None => HttpResponse::Conflict().body("The pipeline has been terminated"),
    }
}

#[get("/pause")]
async fn pause(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            controller.pause();
            HttpResponse::Ok().body("Pipeline paused")
        }
        None => HttpResponse::Conflict().body("The pipeline has been terminated"),
    }
}

#[get("/status")]
async fn status(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            let json_string = serde_json::to_string(controller.status()).unwrap();
            HttpResponse::Ok()
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        }
        None => HttpResponse::Conflict().body("The pipeline has been terminated"),
    }
}

#[get("/shutdown")]
async fn shutdown(state: WebData<ServerState>) -> impl Responder {
    let controller = state.controller.lock().unwrap().take();
    if let Some(controller) = controller {
        match controller.stop() {
            Ok(()) => HttpResponse::Ok().body("Pipeline terminated"),
            Err(e) => HttpResponse::InternalServerError()
                .body(format!("Failed to terminate the pipeline: {e}")),
        }
    } else {
        HttpResponse::Ok().body("Pipeline already terminated")
    }
}

#[cfg(test)]
#[cfg(feature = "with-kafka")]
mod test_with_kafka {
    use super::{build_app, ServerState};
    use crate::{
        test::{
            generate_test_batches,
            kafka::{BufferConsumer, KafkaResources, TestProducer},
            test_circuit, wait, TEST_LOGGER,
        },
        Controller, ControllerConfig, ControllerError,
    };
    use actix_web::{http::StatusCode, middleware::Logger, test, web::Data as WebData, App};
    use crossbeam::queue::SegQueue;
    use log::{error, LevelFilter};
    use proptest::{
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };
    use std::{sync::Arc, thread::sleep, time::Duration};

    #[actix_web::test]
    async fn test_server() {
        // We cannot use proptest macros in `async` context, so generate
        // some random data manually.
        let mut runner = TestRunner::default();
        let data = generate_test_batches(100, 1000)
            .new_tree(&mut runner)
            .unwrap()
            .current();

        let _ = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

        // Create topics.
        let kafka_resources = KafkaResources::create_topics(&[
            ("test_server_input_topic", 1),
            ("test_server_output_topic", 1),
        ]);

        // Create buffer consumer
        let buffer_consumer = BufferConsumer::new("test_server_output_topic");

        // Config string
        let config_str = format!(
            r#"
inputs:
    test_input1:
        transport:
            name: kafka
            config:
                bootstrap.servers: "localhost"
                auto.offset.reset: "earliest"
                topics: [test_server_input_topic]
                log_level: debug
        format:
            name: csv
            config:
                input_stream: test_input1
outputs:
    test_output2:
        stream: test_output1
        transport:
            name: kafka
            config:
                bootstrap.servers: "localhost"
                topic: test_server_output_topic
                max_inflight_messages: 0
        format:
            name: csv
"#
        );

        // Create circuit
        println!("Creating circuit");
        let (circuit, catalog) = test_circuit(4);

        let errors = Arc::new(SegQueue::new());
        let errors_clone = errors.clone();

        let config: ControllerConfig = serde_yaml::from_str(&config_str).unwrap();
        let controller = Controller::with_config(
            circuit,
            catalog,
            &config,
            Box::new(move |e| {
                error!("{e}");
                errors_clone.push(e);
            }) as Box<dyn Fn(ControllerError) + Send + Sync>,
        )
        .unwrap();

        // Create service
        println!("Creating HTTP server");
        let state = WebData::new(ServerState::new(controller));
        let app =
            test::init_service(build_app(App::new().wrap(Logger::default()), state.clone())).await;

        // Write data to Kafka.
        println!("Send test data");
        let producer = TestProducer::new();
        producer.send_to_topic(&data, "test_server_input_topic");

        sleep(Duration::from_millis(2000));
        assert!(buffer_consumer.is_empty());

        // Start command; wait for data.
        println!("/start");
        let req = test::TestRequest::get().uri("/start").to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("/status");
        let req = test::TestRequest::get().uri("/status").to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        // Pause command; send more data, receive none.
        println!("/pause");
        let req = test::TestRequest::get().uri("/pause").to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        sleep(Duration::from_millis(1000));

        producer.send_to_topic(&data, "test_server_input_topic");
        sleep(Duration::from_millis(2000));
        assert_eq!(buffer_consumer.len(), 0);

        // Start; wait for data
        println!("/start");
        let req = test::TestRequest::get().uri("/start").to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("Testing invalid input");
        producer.send_string("invalid\n", "test_server_input_topic");
        wait(|| errors.len() == 1, None);

        // Shutdown
        println!("/shutdown");
        let req = test::TestRequest::get().uri("/shutdown").to_request();
        let resp = test::call_service(&app, req).await;
        // println!("Response: {resp:?}");
        assert!(resp.status().is_success());

        // Start after shutdown must fail.
        println!("/start");
        let req = test::TestRequest::get().uri("/start").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        drop(buffer_consumer);
        drop(kafka_resources);
    }
}
