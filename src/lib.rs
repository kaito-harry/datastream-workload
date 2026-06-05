//! DuplexStreamService per STREAM_CAPABILITY_VERIFICATION.zh.md
//!
//! Flow:
//! 1. Client calls StartDuplexStream(session_id, c2s_id, chunk_count, payload_mode)
//! 2. Service registers c2s callback → creates s2c_id → returns (session_id, c2s_id, s2c_id, "ok")
//! 3. Client registers s2c callback
//! 4. Client sends chunks on c2s → service echos ack on s2c (seq + 1000)
//! 5. Client verifies acks
//! 6. Client calls FinishDuplexStream → service unregisters c2s → returns counts
//! 7. Client unregisters s2c

use actr_framework::{entry, Context, Dest};
use actr_protocol::{ActorResult, ActrId, DataStream, PayloadType};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

mod generated;
use generated::{duplex_stream_actor::*, local::*};

struct SessionState { chunks_received: u32, chunks_echoed: u32 }

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
        &self, req: StartDuplexStreamRequest, ctx: &C,
    ) -> ActorResult<StartDuplexStreamResponse> {
        let c2s = req.client_to_service_stream_id.clone();
        let sid = req.session_id.clone();
        let count = req.client_chunk_count;

        // Service creates s2c stream id
        let s2c = format!("s2c-{}", sid);

        let payload_mode = req.payload_mode();
        let echo_pt = match payload_mode {
            StreamPayloadMode::StreamReliable => PayloadType::StreamReliable,
            StreamPayloadMode::StreamLatencyFirst => PayloadType::StreamLatencyFirst,
        };

        // Clone ctx for use in stream callback
        let ctx_c = ctx.clone();
        let sessions = self.sessions.clone();
        let c2s_c = c2s.clone();
        let s2c_c = s2c.clone();

        ctx.register_stream(c2s.clone(), move |chunk: DataStream, sender: ActrId| {
            let sessions = sessions.clone();
            let c2s = c2s_c.clone();
            let s2c = s2c_c.clone();
            let ctx = ctx_c.clone();

            Box::pin(async move {
                let seq = chunk.sequence;
                let sid_val = chunk.metadata.iter().find(|m| m.key == "session_id").map(|m| m.value.clone()).unwrap_or_default();

                // Build ack metadata per doc
                let meta = vec![
                    actr_protocol::MetadataEntry { key: "session_id".into(), value: sid_val },
                    actr_protocol::MetadataEntry { key: "direction".into(), value: "service-to-client".into() },
                    actr_protocol::MetadataEntry { key: "ack_for_sequence".into(), value: seq.to_string() },
                    actr_protocol::MetadataEntry { key: "source_stream_id".into(), value: c2s.clone() },
                ];

                let ack = DataStream {
                    stream_id: s2c.clone(), sequence: seq + 1000,
                    payload: chunk.payload.clone(), metadata: meta, timestamp_ms: None,
                };

                if let Err(e) = ctx.send_data_stream(&Dest::Actor(sender), ack, echo_pt).await {
                    tracing::error!("Echo failed: {:?}", e);
                } else {
                    let mut s = sessions.lock().await;
                    if let Some(st) = s.get_mut(&c2s) { st.chunks_echoed += 1; }
                }

                {
                    let mut s = sessions.lock().await;
                    if let Some(st) = s.get_mut(&c2s) { st.chunks_received += 1; }
                }
                Ok(())
            })
        }).await?;

        self.sessions.lock().await.insert(c2s.clone(), SessionState { chunks_received: 0, chunks_echoed: 0 });

        Ok(StartDuplexStreamResponse {
            session_id: sid,
            accepted_client_to_service_stream_id: c2s,
            service_to_client_stream_id: s2c,
            status: "ok".into(),
        })
    }

    async fn finish_duplex_stream<C: Context>(
        &self, req: FinishDuplexStreamRequest, ctx: &C,
    ) -> ActorResult<FinishDuplexStreamResponse> {
        ctx.unregister_stream(&req.client_to_service_stream_id).await?;
        let state = { self.sessions.lock().await.remove(&req.client_to_service_stream_id) };
        let (recv, sent) = state.map(|s| (s.chunks_received, s.chunks_echoed)).unwrap_or((0, 0));
        Ok(FinishDuplexStreamResponse {
            session_id: req.session_id,
            client_chunks_received: recv,
            service_chunks_sent: sent,
            status: "ok".into(),
        })
    }
}

entry!(
    DuplexStreamServiceWorkload<DuplexStreamService>,
    DuplexStreamServiceWorkload::new(DuplexStreamService::new())
);
