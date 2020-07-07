package com.mhj;

import java.util.ArrayList;
import java.util.List;

public class TransferUtil {

  static String FFMPEG_PATH = "H:\\ffmpeg\\bin\\ffmpeg.exe";
  /**
   * 视频转换
   *
   * @param ffmpegPath ffmpeg路径
   * @param oldPath 原视频地址
   * @param newPath 新视频存放地址(包含视频格式)
   * @param resolution 分辨率
   * @return
   * @throws FFmpegException
   */
  public static Boolean transform(
      String ffmpegPath, String oldPath, String newPath, String resolution) throws FFmpegException {
    List<String> command = getFfmpegCommand(ffmpegPath, oldPath, newPath, resolution);
    if (null != command && command.size() > 0) {
      return process(command);
    }
    return false;
  }

  private static boolean process(List<String> command) throws FFmpegException {
    try {
      if (null == command || command.size() == 0) return false;
      Process videoProcess = new ProcessBuilder(command).redirectErrorStream(true).start();
      videoProcess.getInputStream().close();
      int exitcode = videoProcess.waitFor();
      if (exitcode == 1) return false;
      return true;
    } catch (Exception e) {
      throw new FFmpegException("file transfer failed", e);
    }
  }

  private static List<String> getFfmpegCommand(
      String ffmpegPath, String oldfilepath, String outputPath, String resolution)
      throws FFmpegException {
    List<String> command = new ArrayList<String>();
    command.add(ffmpegPath); // 添加转换工具路径
    command.add("-i"); // 添加参数＂-i＂，该参数指定要转换的文件
    command.add(oldfilepath); // 添加要转换格式的视频文件的路径

    command.add("-s"); // 设置分辨率
    command.add(resolution);
    command.add("-y"); // 添加参数＂-y＂，该参数指定将覆盖已存在的文件
    command.add(outputPath);
    return command;
  }
}

class FFmpegException extends Exception {

  private static final long serialVersionUID = 1L;

  public FFmpegException() {
    super();
  }

  public FFmpegException(String message) {
    super(message);
  }

  public FFmpegException(Throwable cause) {
    super(cause);
  }

  public FFmpegException(String message, Throwable cause) {
    super(message, cause);
  }
}
