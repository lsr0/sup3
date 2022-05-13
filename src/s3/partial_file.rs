use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

pub struct PartialFile {
    pub writer: tokio::io::BufWriter<tokio::fs::File>,
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
            writer: tokio::io::BufWriter::new(local_file),
            path_partial,
            path_final,
        })
    }
    pub async fn finished(self) -> Result<PathBuf, super::Error> {
        tokio::fs::rename(&self.path_partial, &self.path_final).await?;
        Ok(self.path_final)
    }
    pub async fn cancelled(self) -> Result<(), super::Error> {
        {
            let mut file = self.writer.into_inner();
            file.flush().await?;
        }
        tokio::fs::remove_file(&self.path_partial).await?;
        Ok(())
    }
    pub fn path_printable(&self) -> std::borrow::Cow<'_, str> {
        self.path_final.to_string_lossy()
    }
}

