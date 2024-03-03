use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axum::extract::Json;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use futures::{stream, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::{Api, Client};
use serde::Deserialize;
use serde::Serialize;

#[tokio::main]
async fn main() {
    let api_routes = Router::new().route("/compute-info/pods", post(get_all_pods_info));
    let app = Router::new().nest("/api", api_routes);

    // run it
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

#[derive(Debug, Serialize, Clone)]
struct PodComputeInfo {
    name: String,
    namespace: String,
    node_name: String,
    maintainer: String,
    containers: Vec<Container>,
    metadata: Option<Metadata>,
}

#[derive(Debug, Serialize, Clone)]
struct Metadata {
    labels: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Serialize, Clone)]
struct ComputeResources {
    requested_cpu: Quantity,
    requested_memory: Quantity,
}

#[derive(Debug, Serialize, Clone)]
struct Container {
    name: String,
    image: Option<String>,
    compute_resources: ComputeResources,
}

#[derive(Debug, Deserialize)]
struct PodComputeInfoRequestBody {
    maintainers: Option<Vec<String>>,
    namespaces: Vec<String>,
}

async fn get_all_pods_info(
    Json(request_body): Json<PodComputeInfoRequestBody>,
) -> impl IntoResponse {
    let namespaces = request_body.namespaces;
    let compute_info = get_pods_info(namespaces, Vec::new()).await.unwrap();
    Json(compute_info)
}

async fn get_pods_info(
    namespaces: Vec<String>,
    maintainers: Vec<String>,
) -> anyhow::Result<Vec<PodComputeInfo>> {
    let client = Client::try_default().await?;
    let pods = Arc::new(Mutex::new(Vec::new()));
    stream::iter(namespaces)
        .for_each_concurrent(None, |namespace| {
            let api: Api<Pod> = Api::namespaced(client.clone(), &namespace);
            let pods = Arc::clone(&pods);
            async move {
                let pods_in_namespace = api.list(&Default::default()).await;
                match pods_in_namespace {
                    Ok(p) => {
                        for pod in p.items.into_iter().filter(|pod| {
                            pod.status.as_ref().unwrap().phase.as_ref().unwrap() == "Running"
                        }) {
                            let labels = pod.metadata.labels.unwrap_or_default().clone();
                            let maintainer =
                                labels.get("maintainer").unwrap_or(&"".to_string()).clone();
                            let node_name = pod
                                .spec
                                .as_ref()
                                .unwrap()
                                .node_name
                                .clone()
                                .unwrap_or("".to_string());
                            let raw_containers = pod.spec.as_ref().unwrap().containers.clone();
                            let containers: Vec<Container> = raw_containers
                                .iter()
                                .map(|container| Container {
                                    name: container.name.clone(),
                                    image: container.image.clone(),
                                    compute_resources: ComputeResources {
                                        requested_cpu: container
                                            .resources
                                            .as_ref()
                                            .unwrap()
                                            .requests
                                            .as_ref()
                                            .unwrap()
                                            .get("cpu")
                                            .unwrap()
                                            .clone(),
                                        requested_memory: container
                                            .resources
                                            .as_ref()
                                            .unwrap()
                                            .requests
                                            .as_ref()
                                            .unwrap()
                                            .get("memory")
                                            .unwrap()
                                            .clone(),
                                    },
                                })
                                .collect();

                            let pod_compute_info = PodComputeInfo {
                                name: pod.metadata.name.unwrap(),
                                namespace: pod.metadata.namespace.unwrap(),
                                node_name,
                                maintainer: maintainer.to_string(),
                                containers,
                                metadata: Some(Metadata {
                                    labels: Some(labels),
                                }),
                            };
                            pods.lock().unwrap().push(pod_compute_info);
                        }
                    }
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
        })
        .await;
    Ok(Arc::try_unwrap(pods).unwrap().into_inner().unwrap())
}
