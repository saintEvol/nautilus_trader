//! Factories exposing the Predict data client to live nodes.

use nautilus_common::factories::{ClientConfig, DataClientFactory};
use nautilus_common::live::runner::try_get_data_event_sender;
use nautilus_common::clock::Clock;

use crate::common::consts::PREDICT;
use crate::config::PredictDataClientConfig;
use crate::data::PredictDataClient;

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
        let _ = name;
        let client = PredictDataClient::new(config.clone(), sender)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        PREDICT
    }

    fn config_type(&self) -> &'static str {
        "PredictDataClientConfig"
    }
}
