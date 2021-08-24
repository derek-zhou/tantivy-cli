/// this tantivy command provides a erlang port server

use std::sync::mpsc::{Sender, Receiver, TryRecvError, channel};
use std::thread;
use std::io;
use std::io::prelude::*;
use std::path::PathBuf;
use clap::ArgMatches;
use tantivy::{Term, Document, Index, IndexReader, IndexWriter, ReloadPolicy};
use tantivy::query::QueryParser;
use tantivy::schema::{Field, FieldType, Schema};
use tantivy::collector::TopDocs;

// the message type
enum Message {
    Posted(Box<[u8]>),
    PostedWithData(Box<[u8]>),
    Data(Box<[u8]>),
    DataContinue(Box<[u8]>),
    NonPosted(u32, Box<[u8]>),
    CompletionContinue(u32, Box<[u8]>),
    Completion(u32, Box<[u8]>),
}

#[derive(Clone, Copy, PartialEq)]
enum ReceiverState {
    Init,
    Posted,
}

// the poseted command type
#[derive(Clone, Copy, PartialEq)]
enum PostedCommand {
    Init,
    Add,
    Remove(u64),
}

// the query command type
struct QueryCommand {
    query: String,
    limit: u32,
}

pub fn run_port_cli(matches: &ArgMatches) -> Result<(), String> {
    let directory = PathBuf::from(matches.value_of("index").unwrap());
    let index = Index::open_in_dir(directory).unwrap();
    let schema = index.schema();
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()
	.unwrap();
    let writer = index.writer(50_000_000).unwrap();
    let default_fields: Vec<Field> = schema
        .fields()
        .filter(|&(_, ref field_entry)| match *field_entry.field_type() {
            FieldType::Str(ref text_field_options) => {
                text_field_options.get_indexing_options().is_some()
            }
            _ => false,
        })
        .map(|(field, _)| field)
        .collect();
    let parser = QueryParser::new(schema.clone(),
				  default_fields,
				  index.tokenizers().clone());
    // this is the channel to pass posted requests to the dedicated thread
    let (post_tx, post_rx): (Sender<Message>, Receiver<Message>) = channel();
    // this is the channel to pass completions back
    let (cmpl_tx, cmpl_rx): (Sender<Message>, Receiver<Message>) = channel();
    // use tokio for non-posted requests
    let rt = tokio::runtime::Builder::new_multi_thread()
	.enable_all()
	.build()
	.unwrap();
    let sc = schema.clone();
    // the thread to handle posted requests
    let post_thread = thread::spawn(move || {
	posted_request_thread(post_rx, writer, sc);
    });
    // the thread to handle completions
    let cmpl_thread = thread::spawn(move || {
	completion_thread(cmpl_rx);
    });

    // the receive loop
    let mut state = ReceiverState::Init;
    
    loop {
	match get_request() {
	    Err(e) => {
		if let Some(inner_err) = e.into_inner() {
 		    panic!("Error: {}", inner_err);
		}
		break;
	    },
	    Ok(Message::NonPosted(seq, data)) => {
		let tx = cmpl_tx.clone();
		let rc = reader.clone();
		let sc = schema.clone();
		let pc = parser.clone();
		rt.spawn(async move {
		    handle_non_posted(seq, &data, tx, rc, pc, sc);
		});
		state = ReceiverState::Init;
	    },
	    Ok(m @ Message::Posted(_)) => {
		post_tx.send(m).unwrap();
		state = ReceiverState::Init;
	    },
	    Ok(m @ Message::PostedWithData(_)) => {
		post_tx.send(m).unwrap();
		state = ReceiverState::Posted;
	    },
	    Ok(m @ Message::Data(_)) => {
		if state == ReceiverState::Posted {
		    post_tx.send(m).unwrap();
		    state = ReceiverState::Init;
		} else {
		    panic!("Error: unsupported message");
		}
	    },
	    Ok(m @ Message::DataContinue(_)) => {
		if state == ReceiverState::Posted {
		    post_tx.send(m).unwrap();
		} else {
		    panic!("Error: unsupported message");
		}
	    },
	    Ok(_) => {
		panic!("Error: unsupported message");
	    },	    
	}
    }
    
    drop(cmpl_tx);
    drop(post_tx);
    drop(rt);
    // the two threads I spawn will quit when all tx went out of scope
    cmpl_thread.join().unwrap();
    post_thread.join().unwrap();
    Ok(())
}

fn get_request() -> io::Result<Message> {
    let mut buffer: [u8; 8] = [0; 8];
    
    io::stdin().read_exact(&mut buffer)?;
    let size = ((buffer[0] as u32) << 24) + ((buffer[1] as u32) << 16) +
	((buffer[2] as u32) << 8) + (buffer[3] as u32);
    let seq = ((buffer[5] as u32) << 16) + ((buffer[6] as u32) << 8) + (buffer[7] as u32);

    if size <= 4 {
	Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid packet length"))
    } else {
	let mut v:Vec<u8> = vec![0; (size - 4) as usize];
	io::stdin().read_exact(&mut v)?;
	let data = v.into_boxed_slice();
	
	match buffer[4] as char {
	    'P' => Ok(Message::Posted(data)),
	    'p' => Ok(Message::PostedWithData(data)),
	    'D' => Ok(Message::Data(data)),
	    'd' => Ok(Message::DataContinue(data)),
	    'R' => Ok(Message::NonPosted(seq, data)),
	    _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid packet type")),
	}
    }
}

fn posted_request_thread(rx: Receiver<Message>, mut writer: IndexWriter,
			 schema: Schema) {
    let mut dirty = false;
    let mut command = PostedCommand::Init;
    
    loop {
	if dirty {
	    match rx.try_recv() {
		Err(TryRecvError::Empty) => {
		    writer.commit().unwrap();
		    dirty = false;
		},
		Err(TryRecvError::Disconnected) => {
		    writer.commit().unwrap();
		    break;
		},
		Ok(m) => {
		    command = handle_posted_message(&m, command, &mut writer, &schema);
		    dirty = true;
		}
	    }
	} else {
	    match rx.recv() {
		Err(_) => {
		    break;
		},
		Ok(m) => {
		    command = handle_posted_message(&m, command, &mut writer, &schema);
		    dirty = true;
		}
	    }
	}
    }
}

fn handle_posted_message(m: &Message, command: PostedCommand, writer: &mut IndexWriter,
			 schema: &Schema) -> PostedCommand {
    match m {
	Message::Posted(data) => {
	    run_posted_command(parse_posted_command(&data), writer, schema);
	    PostedCommand::Init
	},
	Message::PostedWithData(data) => {
	    if command == PostedCommand::Init {
		parse_posted_command(&data)
	    } else {
		panic!("command ongoing")
	    }
	}
	Message::Data(data) => {
	    if command == PostedCommand::Init {
		panic!("no command")
	    } else {
		let s = std::str::from_utf8(&data).unwrap();
		let doc = schema.parse_document(s).unwrap();
		run_posted_command_with_doc(command, doc, writer, schema);
		PostedCommand::Init
	    }
	}
	Message::DataContinue(data) => {
	    if command == PostedCommand::Init {
		panic!("no command")
	    } else {
		let s = std::str::from_utf8(&data).unwrap();
		let doc = schema.parse_document(s).unwrap();
		run_posted_command_with_doc(command, doc, writer, schema);
		command
	    }
	}
	_ => panic!("illegal command"),
    }
}

fn completion_thread(rx: Receiver<Message>) {
    loop {
	match rx.recv() {
	    Err(_) => break,
	    Ok(Message::Completion(seq, data)) =>
		send_data('C' as u8, seq, &data),
	    Ok(Message::CompletionContinue(seq, data)) =>
		send_data('c' as u8, seq, &data),
	    Ok(_) => break,
	}
    }
}

fn send_data(t: u8, seq: u32, data: &[u8]) {
    let mut buffer: [u8; 8] = [0; 8];
    let len = data.len() + 4;
    buffer[0] = (len >> 24) as u8;
    buffer[1] = (len >> 16) as u8;
    buffer[2] = (len >> 8) as u8;
    buffer[3] = len as u8;
    buffer[4] = t;
    buffer[5] = (seq >> 16) as u8;
    buffer[6] = (seq >> 8) as u8;
    buffer[7] = seq as u8;
    io::stdout().write_all(&buffer).unwrap();
    if len > 4 {
	io::stdout().write_all(data).unwrap();
    }
    io::stdout().flush().unwrap();
}

fn handle_non_posted(seq: u32, data: &[u8], tx: Sender<Message>,
		     reader: IndexReader, parser: QueryParser, schema: Schema) {
    let command: QueryCommand = parse_query_command(data);
    let searcher = reader.searcher();

    if let Ok(query) = parser.parse_query(&command.query) {
	if let Ok(top_docs) = searcher.search(&query,
					      &TopDocs::with_limit(command.limit as usize)) {
	    let len = top_docs.len();
	    if len == 0 {
		reply_dummy(seq, &tx)
	    } else {
		for i in 0 .. len {
		    let (_score, addr) = top_docs[i];
		    let doc = searcher.doc(addr).unwrap();
		    let data = schema
			.to_json(&doc)
			.into_boxed_str()
			.into_boxed_bytes();
		    if i == len - 1 {
			tx.send(Message::Completion(seq, data)).unwrap();
		    } else {
			tx.send(Message::CompletionContinue(seq, data)).unwrap();
		    }				
		}
	    }
	} else {
	    reply_dummy(seq, &tx)
	}
    } else {
	reply_dummy(seq, &tx)
    }
}

fn reply_dummy(seq: u32, tx: &Sender<Message>) {
    let v: Vec<u8> = Vec::new();
    tx.send(Message::Completion(seq, v.into_boxed_slice())).unwrap();
}

fn parse_query_command(data: &[u8]) -> QueryCommand {
    let v :serde_json::Value = match serde_json::from_slice(data) {
	Err(_) => panic!("malformed json"),
	Ok(x) => x,
    };
    let q = match v.get("search") {
	None => panic!("no search query"),
	Some(s) => match s.as_str() {
	    None => panic!("bad query str"),
	    Some(s) => String::from(s),
	}
    };
    let l = match v.get("limit") {
	None => 25,
	Some(n) => match n.as_u64() {
	    None => 25,
	    Some(n) => n as u32,
	}
    };
    QueryCommand { query: q, limit: l}
}

fn parse_posted_command(data: &[u8]) -> PostedCommand {
    let v :serde_json::Value = match serde_json::from_slice(data) {
	Err(_) => panic!("malformed json"),
	Ok(x) => x,
    };
    if let Some(_) = v.get("add") {
	PostedCommand::Add
    } else if let Some(n) = v.get("remove") {
	match n.as_u64() {
	    None => panic!("no id to remove"),
	    Some(i) => PostedCommand::Remove(i),
	}
    } else {
	panic!("no valid command")
    }
}

fn run_posted_command_with_doc(command: PostedCommand, doc: Document,
			       writer: &mut IndexWriter, schema: &Schema) {
    match command {
	PostedCommand::Add => {
	    writer.add_document(doc);
	}
	PostedCommand::Remove(id) => {
	    let id_field = schema.get_field("id").unwrap();
	    writer.delete_term(Term::from_field_u64(id_field, id));
	    writer.add_document(doc);
	}
	_ => panic!("unsupported command"),
    }
}

fn run_posted_command(command: PostedCommand, writer: &mut IndexWriter, schema: &Schema) {
    match command {
	PostedCommand::Remove(id) => {
	    let id_field = schema.get_field("id").unwrap();
	    writer.delete_term(Term::from_field_u64(id_field, id));
	}
	_ => panic!("unsupported command"),
    }
}
