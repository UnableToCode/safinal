package com.mhj.video;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Domain object representing a company employee. Project Lombok keeps actual code at a minimum.
 * {@code @Data} - Generates getters, setters, toString, hash, and equals functions {@code @Entity}
 * - JPA annotation to flag this class for DB persistence {@code @NoArgsConstructor} - Create a
 * constructor with no args to support JPA {@code @AllArgsConstructor} - Create a constructor with
 * all args to support testing {@code @JsonIgnoreProperties(ignoreUnknow=true)} When converting JSON
 * to Java, ignore any unrecognized attributes. This is critical for REST because it encourages
 * adding new fields in later versions that won't break. It also allows things like _links to be
 * ignore as well, meaning HAL documents can be fetched and later posted to the server without
 * adjustment.
 *
 * @author Greg Turnquist
 */
@Data
@Entity
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor
class Video {

  @Id @GeneratedValue private Long id;
  private String videoTitle;
  private String uploadDate;
  private String url360;
  private String url480;

  /**
   * Useful constructor when id is not yet known. @Param videoTitle
   *
   * @param uploadDate 上传日期
   * @param url360 360p视频地址
   * @param url480 480p视频地址
   */
  public Video(String videoTitle, String uploadDate, String url360, String url480) {
    this.videoTitle = videoTitle;
    this.uploadDate = uploadDate;
    this.url360 = url360;
    this.url480 = url480;
  }

  public String getUrl360() {
    return url360;
  }

  public String getUrl480() {
    return url480;
  }
}
