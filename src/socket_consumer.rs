use crate::{
    BroadcastRequest, Log, Message, RequestBody, RequestMessage, ResponseBody, ResponseMessage,
    ResponsePayload,
    broadcast::{MessageBroadcast, Syncable, handle_broadcast_request},
};

pub fn handle_socket_message(
    message: Message,
    node_id: &str,
    local_msg_id: &mut u32,
    message_broadcast: &mut MessageBroadcast,
) -> Option<Log> {
    match message {
        Message::Log(log) => Some(Log::Log(log)),
        Message::Request(BroadcastRequest { dest, sync }) => {
            let payload = handle_broadcast_request(sync);
            let message = RequestMessage {
                src: node_id.to_string(),
                dest: dest,
                body: RequestBody {
                    msg_id: *local_msg_id,
                    payload,
                },
            };
            message_broadcast.sent(&message);

            *local_msg_id += 1;
            Some(Log::Request(message))
        }
        Message::Response { payload, request } => {
            let message = ResponseMessage {
                src: node_id.to_string(),
                dest: request.src,
                body: ResponseBody {
                    msg_id: *local_msg_id,
                    in_reply_to: request.body.msg_id,
                    payload,
                },
            };
            *local_msg_id += 1;
            Some(Log::Response(message))
        }
        Message::Ack(msg) => {
            message_broadcast.ack(&msg.src, msg.body.in_reply_to);
            if let ResponsePayload::SyncOk { messages } = &msg.body.payload {
                message_broadcast.ack_sync(msg.src.clone(), messages.clone());
            }
            Some(Log::Log(format!(
                "<- {}",
                serde_json::to_string(&msg).unwrap()
            )))
        }
        Message::Retry => {
            let messages = message_broadcast.sync();
            let requests = messages
                .into_iter()
                .map(|msg| {
                    let req = RequestMessage {
                        src: node_id.to_string(),
                        dest: msg.dest,
                        body: RequestBody {
                            msg_id: *local_msg_id,
                            payload: msg.sync,
                        },
                    };
                    message_broadcast.sent(&req);

                    *local_msg_id += 1;
                    req
                })
                .collect();
            Some(Log::Requests(requests))
        }
    }
}
