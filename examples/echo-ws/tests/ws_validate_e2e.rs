//! Validation middleware over the real stack, zero-registration mode: PGV
//! rules from `protocols/testing/v1/validated.proto` are enforced via the
//! embedded FILE_DESCRIPTOR_SET (reflection — no per-method code), and the
//! caster ships a custom details payload in `grpc-status-details-bin` that
//! the client decodes back — the komeet-style domain-error flow.

use prost::Message as _;
use slozhn::middleware::ValidateLayer;
use slozhn_proto::testing::v1::validated_client::ValidatedClient;
use slozhn_proto::testing::v1::validated_server::{Validated, ValidatedServer};
use slozhn_proto::testing::v1::{ValidatedReply, ValidatedRequest};
use tonic::{Request, Response, Status};
use tower::Layer as _;

struct ValidatedImpl;

#[tonic::async_trait]
impl Validated for ValidatedImpl {
    async fn check(
        &self,
        req: Request<ValidatedRequest>,
    ) -> Result<Response<ValidatedReply>, Status> {
        Ok(Response::new(ValidatedReply { echo: req.into_inner().name }))
    }

    async fn check_stream(
        &self,
        req: Request<tonic::Streaming<ValidatedRequest>>,
    ) -> Result<Response<ValidatedReply>, Status> {
        use tokio_stream::StreamExt as _;
        let mut inbound = req.into_inner();
        let mut last = String::new();
        while let Some(m) = inbound.next().await {
            last = m?.name;
        }
        Ok(Response::new(ValidatedReply { echo: last }))
    }
}

/// Stand-in for a domain error proto (komeet's DomainError.validation):
/// what matters is that the caster fully controls the details bytes.
#[derive(Clone, PartialEq, prost::Message)]
struct DomainViolations {
    #[prost(string, repeated, tag = "1")]
    fields: Vec<String>,
}

async fn start_server(layer: ValidateLayer<prost_validate::Error>) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let routes = tonic::service::Routes::new(ValidatedServer::new(ValidatedImpl));
        let validated = layer.layer(routes);
        let app = axum::Router::new().route("/rpc", slozhn::server::grpc_ws(validated));
        let _ = axum::serve(listener, app).await;
    });
    addr
}

fn reflect_layer() -> ValidateLayer<prost_validate::Error> {
    // per-package descriptor sets: dependencies first
    ValidateLayer::from_descriptor_sets([
        slozhn_proto::validate::FILE_DESCRIPTOR_SET,
        slozhn_proto::testing::v1::FILE_DESCRIPTOR_SET,
    ])
    .expect("descriptor set with validate extensions")
}

fn client(addr: std::net::SocketAddr) -> ValidatedClient<slozhn::client::Channel> {
    ValidatedClient::new(slozhn::client::builder(format!("ws://{addr}/rpc")).build())
}

fn valid() -> ValidatedRequest {
    ValidatedRequest { name: "alice".into(), count: 1, email: String::new() }
}

#[tokio::test]
async fn valid_request_passes() {
    let addr = start_server(reflect_layer()).await;
    let resp = client(addr).check(Request::new(valid())).await.unwrap();
    assert_eq!(resp.into_inner().echo, "alice");
}

#[tokio::test]
async fn invalid_request_rejected_with_default_caster() {
    let addr = start_server(reflect_layer()).await;

    // name too short (min_len 3)
    let err = client(addr)
        .check(Request::new(ValidatedRequest { name: "x".into(), count: 1, email: String::new() }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("name"), "field named in message: {}", err.message());

    // count must be > 0
    let err = client(addr)
        .check(Request::new(ValidatedRequest { name: "alice".into(), count: 0, email: String::new() }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // bad email
    let err = client(addr)
        .check(Request::new(ValidatedRequest {
            name: "alice".into(),
            count: 1,
            email: "not-an-email".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn custom_caster_details_round_trip() {
    let layer = reflect_layer().caster(|_method, violations| {
        let domain = DomainViolations {
            fields: violations.iter().map(|e| e.field.clone()).collect(),
        };
        tonic::Status::with_details(
            tonic::Code::InvalidArgument,
            "validation failed",
            domain.encode_to_vec().into(),
        )
    });
    let addr = start_server(layer).await;

    let err = client(addr)
        .check(Request::new(ValidatedRequest { name: "x".into(), count: 1, email: String::new() }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "validation failed");
    let domain = DomainViolations::decode(err.details()).expect("details decode");
    assert_eq!(domain.fields.len(), 1);
    assert!(domain.fields[0].contains("name"), "field path in details: {:?}", domain.fields);
}

#[tokio::test]
async fn stream_message_validated_mid_stream() {
    let addr = start_server(reflect_layer()).await;

    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut c = client(addr);
    let call = tokio::spawn(async move { c.check_stream(Request::new(outbound)).await });

    tx.send(valid()).await.unwrap();
    tx.send(ValidatedRequest { name: "x".into(), count: 1, email: String::new() })
        .await
        .unwrap();
    drop(tx);

    let err = call.await.unwrap().unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "second stream message rejected");
}
