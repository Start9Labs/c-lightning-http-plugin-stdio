use failure::Error;
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct JSONRPCv2;
impl serde::Serialize for JSONRPCv2 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("2.0")
    }
}
impl<'de> serde::Deserialize<'de> for JSONRPCv2 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let version: Option<String> = serde::Deserialize::deserialize(deserializer)?;
        match version {
            Some(a) if a != "2.0" => {
                return Err(serde::de::Error::custom(format!(
                    "invalid RPC version: {}",
                    a
                )))
            }
            _ => (),
        }
        Ok(JSONRPCv2)
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct RPCReq {
    pub id: usize,
    pub jsonrpc: JSONRPCv2,
    pub method: String,
    pub params: Vec<Value>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct RPCRes {
    pub id: usize,
    pub jsonrpc: JSONRPCv2,
    pub result: RPCResult,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RPCResult {
    Result(Value),
    Error(Value),
}
impl RPCResult {
    pub fn res(self) -> Result<Value, Value> {
        self.into()
    }
}
impl From<RPCResult> for Result<Value, Value> {
    fn from(r: RPCResult) -> Self {
        match r {
            RPCResult::Result(a) => Ok(a),
            RPCResult::Error(e) => Err(e),
        }
    }
}

pub fn send_rpc<'a, I: IntoIterator<Item = R>, R: AsRef<RPCReq> + serde::Serialize>(
    reqs: R,
) -> Result<Vec<RPCRes>, serde_json::Error> {
    serde_json::to_writer(std::io::stdout(), &reqs)?;
    serde_json::from_reader(std::io::stdin())
}

fn main() {
    println!("Hello, world!");
}
