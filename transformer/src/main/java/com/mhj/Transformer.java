package com.mhj;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Transformer {

  private  static  final Logger logger = LoggerFactory.getLogger(Transformer.class.getName());

  public static void main(String[] args) {
    while(true){
      List<String> newVideoPaths = Consumer.recvMsg();
      newVideoPaths.forEach(oldFilePath->{
        String newFilePath360 =
            oldFilePath.substring(0, oldFilePath.lastIndexOf('.'))
                + "[360p]"
                + oldFilePath.substring(oldFilePath.lastIndexOf('.'));
        String newFilePath720 =
            oldFilePath.substring(0, oldFilePath.lastIndexOf('.'))
                + "[720p]"
                + oldFilePath.substring(oldFilePath.lastIndexOf('.'));
        try {
          TransferUtil.transform(TransferUtil.FFMPEG_PATH, oldFilePath, newFilePath360, "640x360");
          TransferUtil.transform(TransferUtil.FFMPEG_PATH, oldFilePath, newFilePath720, "1280x720");
        } catch (FFmpegException e) {
          e.printStackTrace();
        }
        logger.info(oldFilePath+"have been transformed.");
      });
    }
  }
}
