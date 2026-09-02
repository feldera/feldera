//! Authentication tests for the NATS input connector's connection options:
//! each supported `auth` method against a real `nats-server` configured to
//! require it, plus the configuration-validation error paths.

use super::super::config_utils::translate_connect_options;
use super::util;
use feldera_types::transport::nats as cfg;

fn connect_options(auth: cfg::Auth) -> cfg::ConnectOptions {
    cfg::ConnectOptions {
        server_url: String::new(),
        auth,
        tls: Default::default(),
        connection_timeout_secs: 5,
        request_timeout_secs: 5,
    }
}

/// Translate `auth` and connect to the server at `addr`, returning whether
/// the connection succeeded.
async fn try_connect(addr: &str, auth: cfg::Auth) -> bool {
    let options = translate_connect_options(&connect_options(auth))
        .await
        .expect("translating valid auth config should succeed");
    async_nats::connect_with_options(addr, options)
        .await
        .is_ok()
}

#[tokio::test]
async fn test_nats_token_auth() {
    let (_guard, addr) = util::start_nats_with_args(&["--auth", "s3cret"]).unwrap();

    assert!(
        try_connect(
            &addr,
            cfg::Auth {
                token: Some("s3cret".to_string()),
                ..Default::default()
            },
        )
        .await,
        "connection with the correct token should succeed"
    );

    assert!(
        !try_connect(
            &addr,
            cfg::Auth {
                token: Some("wrong".to_string()),
                ..Default::default()
            },
        )
        .await,
        "connection with a wrong token should fail"
    );

    assert!(
        !try_connect(&addr, cfg::Auth::default()).await,
        "unauthenticated connection should fail"
    );
}

#[tokio::test]
async fn test_nats_user_password_auth() {
    let (_guard, addr) =
        util::start_nats_with_args(&["--user", "svc", "--pass", "hunter2"]).unwrap();

    assert!(
        try_connect(
            &addr,
            cfg::Auth {
                user_and_password: Some(cfg::UserAndPassword {
                    user: "svc".to_string(),
                    password: "hunter2".to_string(),
                }),
                ..Default::default()
            },
        )
        .await,
        "connection with the correct username and password should succeed"
    );

    assert!(
        !try_connect(
            &addr,
            cfg::Auth {
                user_and_password: Some(cfg::UserAndPassword {
                    user: "svc".to_string(),
                    password: "wrong".to_string(),
                }),
                ..Default::default()
            },
        )
        .await,
        "connection with a wrong password should fail"
    );
}

#[tokio::test]
async fn test_nats_nkey_auth() {
    // A bare NKey user declared in the server configuration (no JWT /
    // operator mode): the server issues a nonce and the client signs it
    // with the seed.
    let key_pair = nkeys::KeyPair::new_user();
    let public_key = key_pair.public_key();
    let seed = key_pair.seed().unwrap();

    let config_dir = tempfile::TempDir::new().unwrap();
    let config_path = config_dir.path().join("auth.conf");
    std::fs::write(
        &config_path,
        format!("authorization {{ users = [ {{ nkey: {public_key} }} ] }}\n"),
    )
    .unwrap();

    let (_guard, addr) =
        util::start_nats_with_args(&["-c", config_path.to_str().unwrap()]).unwrap();

    assert!(
        try_connect(
            &addr,
            cfg::Auth {
                nkey: Some(seed),
                ..Default::default()
            },
        )
        .await,
        "connection with the configured nkey seed should succeed"
    );

    let other_seed = nkeys::KeyPair::new_user().seed().unwrap();
    assert!(
        !try_connect(
            &addr,
            cfg::Auth {
                nkey: Some(other_seed),
                ..Default::default()
            },
        )
        .await,
        "connection with an unknown nkey seed should fail"
    );
}

#[tokio::test]
async fn test_nats_auth_validation_errors() {
    // More than one method.
    let err = translate_connect_options(&connect_options(cfg::Auth {
        token: Some("t".to_string()),
        user_and_password: Some(cfg::UserAndPassword {
            user: "u".to_string(),
            password: "p".to_string(),
        }),
        ..Default::default()
    }))
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("multiple NATS authentication methods"),
        "unexpected error: {err:#}"
    );

    // A JWT without the seed that signs the connection nonce.
    let err = translate_connect_options(&connect_options(cfg::Auth {
        jwt: Some("eyJ...".to_string()),
        ..Default::default()
    }))
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("requires `nkey`"),
        "unexpected error: {err:#}"
    );

    // A JWT with a malformed seed.
    let err = translate_connect_options(&connect_options(cfg::Auth {
        jwt: Some("eyJ...".to_string()),
        nkey: Some("not-a-seed".to_string()),
        ..Default::default()
    }))
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("invalid NATS nkey seed"),
        "unexpected error: {err:#}"
    );

    // `jwt` together with `nkey` is one method, not two.
    let key_pair = nkeys::KeyPair::new_user();
    translate_connect_options(&connect_options(cfg::Auth {
        jwt: Some("eyJ...".to_string()),
        nkey: Some(key_pair.seed().unwrap()),
        ..Default::default()
    }))
    .await
    .expect("jwt + nkey should be accepted as a single method");
}
