use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

pub struct PartialFile {
    pub writer: Option<tokio::io::BufWriter<tokio::fs::File>>,
    path_partial: PathBuf,
    path_final: PathBuf,
}

impl PartialFile {
    pub async fn new(path_final: PathBuf) -> Result<PartialFile, super::Error> {
        let mut path_string_temporary = path_final.as_os_str().to_owned();
        path_string_temporary.push(".sup3.partial");
        let path_partial = std::path::PathBuf::from(path_string_temporary);
        let local_file = tokio::fs::File::create(&path_partial).await?;
        Ok(PartialFile {
            writer: Some(tokio::io::BufWriter::new(local_file)),
            path_partial,
            path_final,
        })
    }
    pub async fn finished(mut self) -> Result<PathBuf, super::Error> {
        self.writer().flush().await?;
        tokio::fs::rename(&self.path_partial, &self.path_final).await?;
        self.writer.take();
        Ok(self.path_final.clone())
    }
    pub async fn cancelled(mut self) -> Result<(), super::Error> {
        self.cancel().await
    }
    async fn cancel(&mut self) -> Result<(), super::Error> {
        {
            let mut file = self.writer.take().expect("not already cancelled").into_inner();
            file.flush().await?;
        }
        tokio::fs::remove_file(&self.path_partial).await?;
        Ok(())
    }
    pub fn path_printable(&self) -> std::borrow::Cow<'_, str> {
        self.path_final.to_string_lossy()
    }
    pub fn writer(&mut self) -> &mut tokio::io::BufWriter<tokio::fs::File> {
        self.writer.as_mut().expect("writer not taken")
    }
}

impl Drop for PartialFile {
    fn drop(&mut self) {
        if self.writer.is_none() {
            return;
        }
        if let Err(e) = futures::executor::block_on(self.cancel()) {
            eprintln!("failed to drop partial file {:?}: {}", self.path_partial, e);
        }
    }
}

