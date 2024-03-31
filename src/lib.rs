use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use error::ResumableIOError;
use futures::{future::select, FutureExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot::{self, Receiver, Sender},
    },
    time::Sleep,
};
mod error;
pub struct ResumableIO<IO> {
    bytes_read: usize,
    bytes_written: usize,
    timeout_duration: Duration,
    error_reporter: UnboundedSender<IntruptedIO<IO>>,
    current_io: ResumableCurrentIO<IO>,
    reliable: bool,
}

impl<IO> ResumableIO<IO>
where
    IO: AsyncRead + AsyncWrite,
{
    pub fn new(
        io: Option<IO>,
        timeout_duration: Duration,
    ) -> (Self, UnboundedReceiver<IntruptedIO<IO>>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Self {
                current_io: io.map(ResumableCurrentIO::Ok).unwrap_or_default(),
                timeout_duration,
                error_reporter: tx,
                bytes_read: 0,
                bytes_written: 0,
                reliable: true,
            },
            rx,
        )
    }
}

impl<IO> AsyncRead for ResumableIO<IO>
where
    IO: AsyncRead + AsyncWrite + std::marker::Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.current_io {
            ResumableCurrentIO::Uninitialized => {
                let e = Arc::new(io::Error::from(io::ErrorKind::NotConnected));
                let (intrupted_io, rx) = IntruptedIO::new(e.clone(), 0, 0, cx.waker().clone());
                self.error_reporter
                    .send(intrupted_io)
                    .or(Err(io::Error::from(e.kind())))?;
                self.current_io = ResumableCurrentIO::Err(
                    e,
                    rx,
                    Box::pin(tokio::time::sleep(self.timeout_duration)),
                );
                Poll::Pending
            }
            ResumableCurrentIO::Ok(ref mut io) => match Pin::new(io).poll_read(cx, buf) {
                Poll::Ready(Ok(_)) => {
                    self.bytes_read += buf.filled().len();
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    let error = Arc::new(e);
                    let (intrupted_io, rx) = IntruptedIO::new(
                        error.clone(),
                        self.bytes_read,
                        self.bytes_written,
                        cx.waker().clone(),
                    );
                    self.error_reporter
                        .send(intrupted_io)
                        .or(Err(io::Error::from(error.kind())))?;
                    self.current_io = ResumableCurrentIO::Err(
                        error,
                        rx,
                        Box::pin(tokio::time::sleep(self.timeout_duration)),
                    );
                    Poll::Pending
                }
                Poll::Pending => Poll::Pending,
            },
            ResumableCurrentIO::Err(e, io_receiver, timeout) => {
                match select(io_receiver, timeout).poll_unpin(cx) {
                    Poll::Ready(either) => match either {
                        futures::future::Either::Left((io, _timeout)) => match io {
                            Ok(Some(io)) => {
                                self.current_io = ResumableCurrentIO::Ok(io);
                                self.poll_read(cx, buf)
                            }
                            Err(_) | Ok(None) => Poll::Ready(Err(io::Error::from(e.kind()))),
                        },
                        futures::future::Either::Right((_timeout, io)) => {
                            io.close();
                            Poll::Ready(Err(io::Error::from(e.kind())))
                        }
                    },
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}

impl<IO> AsyncWrite for ResumableIO<IO>
where
    IO: AsyncRead + AsyncWrite + std::marker::Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.current_io {
            ResumableCurrentIO::Uninitialized => {
                let e = Arc::new(io::Error::from(io::ErrorKind::NotConnected));
                let (intrupted_io, rx) = IntruptedIO::new(e.clone(), 0, 0, cx.waker().clone());
                self.error_reporter
                    .send(intrupted_io)
                    .or(Err(io::Error::from(e.kind())))?;
                self.current_io = ResumableCurrentIO::Err(
                    e,
                    rx,
                    Box::pin(tokio::time::sleep(self.timeout_duration)),
                );
                Poll::Pending
            }
            ResumableCurrentIO::Ok(ref mut io) => match Pin::new(io).poll_write(cx, buf) {
                Poll::Ready(Ok(n)) => {
                    self.bytes_written += n;
                    Poll::Ready(Ok(n))
                }
                Poll::Ready(Err(e)) => {
                    let error = Arc::new(e);
                    let (intrupted_io, rx) = IntruptedIO::new(
                        error.clone(),
                        self.bytes_read,
                        self.bytes_written,
                        cx.waker().clone(),
                    );
                    self.error_reporter
                        .send(intrupted_io)
                        .or(Err(io::Error::from(error.kind())))?;
                    self.current_io = ResumableCurrentIO::Err(
                        error,
                        rx,
                        Box::pin(tokio::time::sleep(self.timeout_duration)),
                    );
                    Poll::Pending
                }
                Poll::Pending => Poll::Pending,
            },
            ResumableCurrentIO::Err(e, io_receiver, timeout) => {
                match select(io_receiver, timeout).poll_unpin(cx) {
                    Poll::Ready(either) => match either {
                        futures::future::Either::Left((io, _timeout)) => match io {
                            Ok(Some(io)) => {
                                self.current_io = ResumableCurrentIO::Ok(io);
                                self.poll_write(cx, buf)
                            }
                            Err(_) | Ok(None) => Poll::Ready(Err(io::Error::from(e.kind()))),
                        },
                        futures::future::Either::Right((_timeout, io)) => {
                            io.close();
                            Poll::Ready(Err(io::Error::from(e.kind())))
                        }
                    },
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.current_io {
            ResumableCurrentIO::Uninitialized => Poll::Ready(Ok(())),
            ResumableCurrentIO::Ok(io) => match Pin::new(io).poll_flush(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => {
                    if self.reliable {
                        return Poll::Ready(Err(e));
                    }
                    let error = Arc::new(e);
                    let (intrupted_io, rx) = IntruptedIO::new(
                        error.clone(),
                        self.bytes_read,
                        self.bytes_written,
                        cx.waker().clone(),
                    );
                    self.error_reporter
                        .send(intrupted_io)
                        .or(Err(io::Error::from(error.kind())))?;
                    self.current_io = ResumableCurrentIO::Err(
                        error,
                        rx,
                        Box::pin(tokio::time::sleep(self.timeout_duration)),
                    );
                    Poll::Pending
                }
                Poll::Pending => Poll::Pending,
            },
            ResumableCurrentIO::Err(e, io_receiver, timeout) => {
                match select(io_receiver, timeout).poll_unpin(cx) {
                    Poll::Ready(either) => match either {
                        futures::future::Either::Left((io, _timeout)) => match io {
                            Ok(Some(io)) => {
                                self.current_io = ResumableCurrentIO::Ok(io);
                                Poll::Ready(Ok(()))
                            }
                            Err(_) | Ok(None) => Poll::Ready(Err(io::Error::from(e.kind()))),
                        },
                        futures::future::Either::Right((_timeout, io)) => {
                            io.close();
                            Poll::Ready(Err(io::Error::from(e.kind())))
                        }
                    },
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.current_io {
            ResumableCurrentIO::Uninitialized => Poll::Ready(Ok(())),
            ResumableCurrentIO::Ok(io) => Pin::new(io).poll_shutdown(cx),
            ResumableCurrentIO::Err(e, _, _) => return Poll::Ready(Err(io::Error::from(e.kind()))),
        }
    }
}
#[derive(Default)]
enum ResumableCurrentIO<IO> {
    #[default]
    Uninitialized,
    Ok(IO),
    Err(Arc<io::Error>, Receiver<Option<IO>>, Pin<Box<Sleep>>),
}

pub struct IntruptedIO<IO> {
    new_io_sender: Option<Sender<Option<IO>>>,
    error: Arc<io::Error>,
    bytes_read: usize,
    bytes_written: usize,
    wake: Waker,
}

impl<IO> IntruptedIO<IO> {
    fn new(
        error: Arc<io::Error>,
        bytes_read: usize,
        bytes_written: usize,
        wake: Waker,
    ) -> (Self, Receiver<Option<IO>>) {
        let (new_io_sender, new_io_receiver) = oneshot::channel();
        (
            Self {
                new_io_sender: Some(new_io_sender),
                error,
                bytes_read,
                bytes_written,
                wake,
            },
            new_io_receiver,
        )
    }
    pub fn send_new_io(mut self, new_io: Option<IO>) -> Result<(), ResumableIOError> {
        let sender = self
            .new_io_sender
            .take()
            .ok_or(ResumableIOError::SenderIsUsed)?;
        sender
            .send(new_io)
            .or(Err(ResumableIOError::ChannelIsClosed))?;
        self.wake.wake();
        Ok(())
    }
    pub fn error(&self) -> &io::Error {
        &self.error
    }
    pub fn bytes_read(&self) -> usize {
        self.bytes_read
    }
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }
}
