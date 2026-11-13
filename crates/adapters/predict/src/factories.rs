//! Factories exposing the Predict data and execution clients to live nodes.

use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_common::live::runner::{try_get_data_event_sender, try_get_exec_event_sender};
use nautilus_common::clock::Clock;

use crate::common::consts::{PREDICT, PREDICT_VENUE};
use crate::config::PredictDataClientConfig;
use crate::data::PredictDataClient;
use crate::execution::client::{PredictExecutionClient, PredictExecutionConfig};

impl ClientConfig for PredictDataClientConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Creates [`PredictDataClient`] instances from a
/// [`PredictDataClientConfig`].
#[derive(Debug, Clone, Copy, Default)]
pub struct PredictDataClientFactory;

impl DataClientFactory for PredictDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: nautilus_common::cache::CacheView,
        _clock: std::rc::Rc<std::cell::RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn nautilus_common::clients::DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<PredictDataClientConfig>()
            .ok_or_else(|| anyhow::anyhow!("config is not a PredictDataClientConfig"))?;
        config.validate()?;
        let sender = try_get_data_event_sender()
            .ok_or_else(|| anyhow::anyhow!("no data event sender on this thread"))?;
        let client = PredictDataClient::new_with_client_id(
            nautilus_model::identifiers::ClientId::from(name),
            config.clone(),
            sender,
            None,
        )?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        PREDICT
    }

    fn config_type(&self) -> &'static str {
        "PredictDataClientConfig"
    }
}

impl ClientConfig for PredictExecutionConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Creates [`PredictExecutionClient`] instances from a
/// [`PredictExecutionConfig`]. Cash/Netting venue semantics (§8.2).
#[derive(Debug, Clone, Copy, Default)]
pub struct PredictExecutionClientFactory;

impl ExecutionClientFactory for PredictExecutionClientFactory {
    fn create(
        &self,
        trader_id: nautilus_model::identifiers::TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: nautilus_common::cache::CacheView,
        _clock: std::rc::Rc<std::cell::RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn nautilus_common::clients::ExecutionClient>> {
        let config = config
            .as_any()
            .downcast_ref::<PredictExecutionConfig>()
            .ok_or_else(|| anyhow::anyhow!("config is not a PredictExecutionConfig"))?;
        config.validate()?;
        let core = nautilus_live::ExecutionClientCore::new(
            trader_id,
            nautilus_model::identifiers::ClientId::from(name),
            *PREDICT_VENUE,
            nautilus_model::enums::OmsType::Netting,
            nautilus_model::identifiers::AccountId::from(format!("PREDICT-{name}")),
            nautilus_model::enums::AccountType::Cash,
            None,
            cache,
        );
        let mut client = PredictExecutionClient::new(core, config.clone())?;
        // factory 期 sender（线程有则装；start() 的 get_exec_event_sender
        // 是权威装接——emitter 契约同 Polymarket）。
        if let Some(sender) = try_get_exec_event_sender() {
            client.set_event_sender(sender);
        }
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        PREDICT
    }

    fn config_type(&self) -> &'static str {
        "PredictExecutionConfig"
    }
}
