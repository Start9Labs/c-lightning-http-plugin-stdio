use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
};

use crossbeam_channel::{Receiver, Sender};
use failure::Error;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use object_pool::Pool;
use serde_json::Value;

pub struct ArcErr<E>(pub Arc<E>);
impl<E> std::fmt::Debug for ArcErr<E>
where
    E: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl<E> std::fmt::Display for ArcErr<E>
where
    E: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl<E> std::clone::Clone for ArcErr<E> {
    fn clone(&self) -> Self {
        ArcErr(self.0.clone())
    }
}
impl<E> std::error::Error for ArcErr<E> where E: std::fmt::Debug + std::fmt::Display {}

#[derive(Clone, Debug)]
pub struct JSONRPCv2;
impl Default for JSONRPCv2 {
    fn default() -> Self {
        JSONRPCv2
    }
}
impl serde::Serialize for JSONRPCv2 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("2.0")
    }
}
impl<'de> serde::Deserialize<'de> for JSONRPCv2 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let version: String = serde::Deserialize::deserialize(deserializer)?;
        match version.as_str() {
            "2.0" => (),
            a => {
                return Err(serde::de::Error::custom(format!(
                    "invalid RPC version: {}",
                    a
                )))
            }
        }
        Ok(JSONRPCv2)
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum SingleOrBatch<T> {
    Single(T),
    Batch(Vec<T>),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct RPCReq {
    #[serde(default)]
    pub id: Value,
    #[serde(default)]
    pub jsonrpc: JSONRPCv2,
    pub method: String,
    pub params: Vec<Value>,
}
impl AsRef<RPCReq> for RPCReq {
    fn as_ref(&self) -> &RPCReq {
        &self
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct RPCRes {
    pub id: Value,
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

#[derive(Clone, Debug)]
pub struct RPCRequester {
    pub index: u16,
    pub req: Sender<SingleOrBatch<RPCReq>>,
    pub res: Receiver<Result<SingleOrBatch<RPCRes>, Error>>,
}

#[derive(Clone, Debug)]
pub struct RPCHandler {
    pub index: u16,
    pub req: Receiver<SingleOrBatch<RPCReq>>,
    pub res: Sender<Result<SingleOrBatch<RPCRes>, Error>>,
}

lazy_static::lazy_static! {
    pub static ref CHANNEL_IDX: AtomicU16 = AtomicU16::new(0);
}

pub fn send_rpc(reqs: &[RPCReq]) -> Result<Vec<RPCRes>, serde_json::Error> {
    serde_json::to_writer(std::io::stdout(), &reqs)?;
    serde_json::from_reader(std::io::stdin())
}

pub fn process_res(
    handlers: &Vec<Option<RPCHandler>>,
    ids: &mut Vec<Option<SingleOrBatch<Value>>>,
    ress_res: Result<Vec<RPCRes>, serde_json::Error>,
) {
    match ress_res {
        Ok(ress) => {
            let mut to_dispatch: Vec<_> = std::iter::repeat_with(|| None)
                .take(handlers.len())
                .collect();
            for mut res in ress {
                let idx = match res.id {
                    Value::Number(n) => match n.as_u64() {
                        Some(n) => n as usize,
                        None => {
                            continue;
                        }
                    },
                    _ => {
                        continue;
                    }
                };
                let id = match ids.get_mut(idx >> 16).and_then(|a| a.take()) {
                    Some(a) => a,
                    None => continue,
                };
                match id {
                    SingleOrBatch::Single(id) => {
                        res.id = id;
                        to_dispatch[idx] = Some(Ok(SingleOrBatch::Single(res)));
                    }
                    SingleOrBatch::Batch(mut ids) => {
                        if let Some(id_slot) = ids.get_mut(idx & 0xffff) {
                            res.id = std::mem::replace(id_slot, serde_json::Value::Bool(false));
                            if let Some(Ok(SingleOrBatch::Batch(ref mut res_batch))) =
                                to_dispatch[idx]
                            {
                                res_batch.push(res)
                            }
                        }
                    }
                }
            }
            for (idx, id) in ids
                .iter()
                .enumerate()
                .filter_map(|(idx, a)| a.as_ref().map(|a| (idx, a)))
            {
                match id {
                    SingleOrBatch::Batch(a)
                        if a.iter()
                            .filter_map(|a| match a {
                                serde_json::Value::Bool(false) => None,
                                _ => Some(()),
                            })
                            .next()
                            .is_none() =>
                    {
                        continue
                    }
                    _ => (),
                };
                if let Some(handler) = handlers.get(idx).and_then(|a| a.as_ref()) {
                    handler
                        .res
                        .send(Err(failure::format_err!("no response recieved")))
                        .unwrap();
                } else {
                    panic!("UNREACHABLE: handler missing for index {}", idx);
                }
            }
            for (idx, res) in to_dispatch
                .into_iter()
                .enumerate()
                .filter_map(|(idx, res)| res.map(|res| (idx, res)))
            {
                if let Some(handler) = handlers.get(idx).and_then(|a| a.as_ref()) {
                    handler.res.send(res).unwrap();
                } else {
                    panic!("UNREACHABLE: handler missing for index {}", idx);
                }
            }
        }
        Err(e) => {
            let e = ArcErr(Arc::new(e));
            for (idx, _) in ids
                .iter()
                .enumerate()
                .filter_map(|(idx, a)| a.as_ref().map(|a| (idx, a)))
            {
                if let Some(handler) = handlers.get(idx).and_then(|a| a.as_ref()) {
                    handler.res.send(Err(Error::from(e.clone()))).unwrap();
                } else {
                    panic!("UNREACHABLE: handler missing for index {}", idx);
                }
            }
        }
    }
}

pub fn pkg_reqs(
    handlers: &Vec<Option<RPCHandler>>,
    ids: &mut Vec<Option<SingleOrBatch<Value>>>,
    reqs: &mut Vec<RPCReq>,
) {
    reqs.clear();
    ids.clear();
    ids.extend(std::iter::repeat(None).take(handlers.len()));
    for (idx, r) in handlers
        .iter()
        .filter_map(|a| a.as_ref())
        .filter_map(|h| h.req.try_recv().ok().map(|r| (h.index, r)))
    {
        match r {
            SingleOrBatch::Single(mut r) => {
                let id = std::mem::replace(
                    &mut r.id,
                    Value::Number(serde_json::Number::from((idx as u32) << 16)),
                );
                ids[idx as usize] = Some(SingleOrBatch::Single(id));
                reqs.push(r)
            }
            SingleOrBatch::Batch(mut rs) => {
                let mut sub_ids = Vec::with_capacity(rs.len());
                for (sub_idx, r) in rs.iter_mut().enumerate() {
                    let id = std::mem::replace(
                        &mut r.id,
                        Value::Number(serde_json::Number::from(
                            (idx as u32) << 16 | sub_idx as u32, // requires sub_idx to fit in u16, meaning batch sizes cannot exceed 65536 requests...lol
                        )),
                    );
                    sub_ids.push(id);
                }
                ids[idx as usize] = Some(SingleOrBatch::Batch(sub_ids));
                reqs.extend(rs);
            }
        }
    }
}

pub fn update_handlers(chan_recv: &Receiver<RPCHandler>, handlers: &mut Vec<Option<RPCHandler>>) {
    if let Ok(handler) = chan_recv.try_recv() {
        if handlers.len() <= handler.index as usize {
            handlers
                .extend(std::iter::repeat(None).take(handler.index as usize + 1 - handlers.len()));
        }
        let idx = handler.index as usize;
        handlers[idx] = Some(handler);
    }
}

pub fn rpc_execute(
    reqs_recv: &Receiver<Vec<RPCReq>>,
    ress_send: &Sender<(Vec<RPCReq>, Result<Vec<RPCRes>, serde_json::Error>)>,
) {
    let reqs = reqs_recv.recv().unwrap();
    let ress = send_rpc(&reqs);
    ress_send.send((reqs, ress)).unwrap();
}

async fn handle(req: Request<Body>) -> Result<Response<Body>, Error> {
    todo!()
}

#[tokio::main]
async fn main() {
    let (chan_send, chan_recv) = crossbeam_channel::unbounded();
    /* It's not super obvious what's happening here, so it merits a prelude.
     * We have 2 threads, henceforth referred to as the RPC Manager Thread and the RPC Executor Thread.
     * The RPC Executor Thread simply recieves a Vec of RPC Requests, writes them to stdout, then
     * sends back the results, along with the buffer allocated for the requests so it can be reused.
     * The RPC Manager Thread bundles up a set of RPC Requests to pass to the executor by recieving
     * all available RPC Requests from each of the HTTP workers. Since this code is our councurrency
     * bottleneck, the objective is to perform our bundling of RPC requests, and our handling of RPC
     * responses while waiting for the RPC executor thread to respond. To do this, we have 2 rotating
     * buffers of requests, A and B. While the request batch B is being sent to the executor, we want
     * to post-process the responses of batch A, then package up a new batch of requests for A. Then,
     * while the request batch A is being sent to the executor, we want to post-process the responses
     * of batch B, then package up a new batch of requests for B.
     */
    let rpc_manager = std::thread::spawn(move || {
        let (reqs_send, reqs_recv) = crossbeam_channel::bounded(1);
        let (ress_send, ress_recv) = crossbeam_channel::bounded(1);
        let mut handlers: Vec<Option<RPCHandler>> = Vec::new();
        let mut ids_a: Vec<Option<SingleOrBatch<Value>>> = Vec::new();
        let mut reqs_a: Vec<RPCReq> = Vec::new();
        let mut ids_b: Vec<Option<SingleOrBatch<Value>>> = Vec::new();
        let mut reqs_b: Vec<RPCReq> = Vec::new();
        let _executor = std::thread::spawn(move || loop {
            rpc_execute(&reqs_recv, &ress_send)
        });
        reqs_send.send(reqs_b).unwrap();
        loop {
            update_handlers(&chan_recv, &mut handlers);
            pkg_reqs(&handlers, &mut ids_a, &mut reqs_a);
            reqs_send.send(reqs_a).unwrap();
            let (reqs_tmp_b, res_b) = ress_recv.recv().unwrap();
            reqs_b = reqs_tmp_b;
            process_res(&handlers, &mut ids_b, res_b);
            pkg_reqs(&handlers, &mut ids_b, &mut reqs_b);
            reqs_send.send(reqs_b).unwrap();
            let (reqs, res_a) = ress_recv.recv().unwrap();
            reqs_a = reqs;
            process_res(&handlers, &mut ids_a, res_a);
        }
    });

    let arc_pool = Arc::new(Pool::new(0, move || {
        let (req_send, req_recv) = crossbeam_channel::unbounded();
        let (res_send, res_recv) = crossbeam_channel::unbounded();
        let chan_idx = CHANNEL_IDX.fetch_add(1, Ordering::SeqCst);
        chan_send
            .send(RPCHandler {
                index: chan_idx,
                req: req_recv,
                res: res_send,
            })
            .unwrap();
        RPCRequester {
            index: chan_idx,
            req: req_send,
            res: res_recv,
        }
    }));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    // And a MakeService to handle each connection...
    let make_service = make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(handle)) });

    // Then bind and serve...
    let server = Server::bind(&addr).serve(make_service);

    // And run forever...
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }

    rpc_manager.join().unwrap()
}
