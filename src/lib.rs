use actr_framework::{entry, Context};
use actr_protocol::ActorResult;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

mod generated;
use generated::{duplex_stream_actor::*, local::*};

struct SessionState { chunks_received: u32 }

pub struct DuplexStreamService {
    sessions: Arc<Mutex<HashMap<String, SessionState>>>,
}

impl DuplexStreamService {
    pub fn new() -> Self {
        Self { sessions: Arc::new(Mutex::new(HashMap::new())) }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl DuplexStreamServiceHandler for DuplexStreamService {
    async fn start_duplex_stream<C: Context>(
        &self,
        req: StartDuplexStreamRequest,
        ctx: &C,
    ) -> ActorResult<StartDuplexStreamResponse> {
        let c2s = req.client_to_service_stream_id.clone();
        let s2c = req.service_to_client_stream_id.clone();
        let sessions = self.sessions.clone();
        let c2s_id = c2s.clone();

        ctx.register_stream(c2s.clone(), move |chunk, _sender| {
            let sessions = sessions.clone();
            let c2s = c2s_id.clone();
            Box::pin(async move {
                {
                    let mut s = sessions.lock().await;
                    s.entry(c2s.clone()).or_insert(SessionState { chunks_received: 0 }).chunks_received += 1;
                }
                Ok(())
            })
        }).await?;

        self.sessions.lock().await.insert(c2s.clone(), SessionState { chunks_received: 0 });
        Ok(StartDuplexStreamResponse {
            ready: true,
            message: format!("c2s={} s2c={} count={}", c2s, s2c, req.chunk_count),
        })
    }

    async fn finish_duplex_stream<C: Context>(
        &self,
        req: FinishDuplexStreamRequest,
        ctx: &C,
    ) -> ActorResult<FinishDuplexStreamResponse> {
        ctx.unregister_stream(&req.client_to_service_stream_id).await?;
        let received = {
            let mut s = self.sessions.lock().await;
            s.remove(&req.client_to_service_stream_id).map(|s| s.chunks_received).unwrap_or(0)
        };
        Ok(FinishDuplexStreamResponse { acknowledged: true, message: format!("received={}", received) })
    }
}

entry!(
    DuplexStreamServiceWorkload<DuplexStreamService>,
    DuplexStreamServiceWorkload::new(DuplexStreamService::new())
);
