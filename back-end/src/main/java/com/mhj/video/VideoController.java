package com.mhj.video;

import static org.springframework.util.StringUtils.getFilename;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.hateoas.CollectionModel;
import org.springframework.hateoas.EntityModel;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@CrossOrigin
public class VideoController {

  private final VideoRepository repository;
  static String filePath = "Z:\\video";
  // 新建视频存储文件夹
  static {
    File dir = new File(filePath);
    if (!dir.exists()) {
      dir.mkdirs();
      System.out.println("mkdir success");
    }
  }

  VideoController(VideoRepository repository) {
    this.repository = repository;
  }
  // 新建视频存储文件夹
  /**
   * Look up all students, and transform them into a REST collection resource. Then return them
   * through Spring Web's {@link ResponseEntity} fluent API.
   */
  @GetMapping("/videoList")
  ResponseEntity<CollectionModel<EntityModel<Video>>> findAll() {

    List<EntityModel<Video>> videos =
        StreamSupport.stream(repository.findAll().spliterator(), false)
            .map(video -> new EntityModel<>(video)) //
            .collect(Collectors.toList());

    return ResponseEntity.ok( //
        new CollectionModel<>(videos));
  }

  @PostMapping("/upload")
  ResponseEntity<?> upload(
      @RequestBody MultipartFile file, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    System.out.println("filename:" + file.getOriginalFilename());
    // 新建视频存储文件夹

    try {
      String filename = getFilename(file.getOriginalFilename());
      File dest = new File(filePath, filename);
      file.transferTo(dest);

      Date d = new Date();
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      String datetime = sdf.format(d);
      String oldFilePath = dest.getPath();
      String newFilePath360 =
          oldFilePath.substring(0, oldFilePath.lastIndexOf('.'))
              + "[360p]"
              + oldFilePath.substring(oldFilePath.lastIndexOf('.'));
      String newFilePath720 =
          oldFilePath.substring(0, oldFilePath.lastIndexOf('.'))
              + "[720p]"
              + oldFilePath.substring(oldFilePath.lastIndexOf('.'));
      Video video =
          new Video(
              filename.substring(0, filename.lastIndexOf(".")),
              datetime,
              newFilePath360,
              newFilePath720);
      Producer.sendMsg(oldFilePath);
      repository.save(video);
      return ResponseEntity.ok().build();
    } catch (IOException e) {
      e.printStackTrace();
      return ResponseEntity.badRequest().body("Unable to upload this video");
    }
  }

  @GetMapping(value = "/preview/360/{id}")
  public @ResponseBody void video360(@PathVariable Long id, HttpServletResponse response)
      throws Exception {
    if (repository.findById(id).isPresent()) {
      Video video = repository.findById(id).get();
      String path = video.getUrl360();
      File file = new File(path);
      FileInputStream in = new FileInputStream(file);
      ServletOutputStream out = response.getOutputStream();
      byte[] b = null;
      while (in.available() > 0) {
        if (in.available() > 10240) {
          b = new byte[10240];
        } else {
          b = new byte[in.available()];
        }
        in.read(b, 0, b.length);
        out.write(b, 0, b.length);
      }
      in.close();
      out.flush();
      out.close();
    }
  }

  @GetMapping(value = "/preview/480/{id}")
  public @ResponseBody void video480(@PathVariable Long id, HttpServletResponse response)
      throws Exception {
    if (repository.findById(id).isPresent()) {
      Video video = repository.findById(id).get();
      String path = video.getUrl480();
      File file = new File(path);
      FileInputStream in = new FileInputStream(file);
      ServletOutputStream out = response.getOutputStream();
      byte[] b = null;
      while (in.available() > 0) {
        if (in.available() > 10240) {
          b = new byte[10240];
        } else {
          b = new byte[in.available()];
        }
        in.read(b, 0, b.length);
        out.write(b, 0, b.length);
      }
      in.close();
      out.flush();
      out.close();
    }
  }
}
