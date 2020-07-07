<template>
  <div class='upload_page'>
    <h3 class='page_title'>上传视频</h3>
    <div align="left"
         style="margin-left:50px">
      <el-upload class="upload"
                 ref="upload"
                 accept=".mp4,  .MP4"
                 :action="uploadUrl"
                 :limit=1
                 :on-change="handleChange"
                 :on-exceed="handleExceed"
                 :on-success="handleSuccess"
                 :on-error="handleError"
                 :file-list="fileList"
                 :auto-upload="false">
        <el-button slot="trigger"
                   size="small"
                   type="primary"
                   icon="el-icon-document"
                   :disabled="btnUploadEnable">选取文件</el-button>
        <el-button style="margin-left: 10px;"
                   size="small"
                   type="success"
                   icon="el-icon-upload"
                   @click="submitUpload">上传到服务器</el-button>
        <div slot="tip"
             class="el-upload__tip">只能上传mp4文件, 且只能上传500MB大小以内文件</div>
      </el-upload>
    </div>
  </div>
</template>

<script>
export default {
  data() {
    return {
      uploadUrl: 'http://localhost:8080/upload',
      videoFlag: false,
      btnUploadEnable: false,
      fileList: []
    }
  },

  methods: {
    submitUpload() {
      this.$refs.upload.submit()
    },
    handleExceed(files, fileList) {
      this.$message.warning(`当前限制选择 1 个文件，本次选择了 ${files.length} 个文件，共选择了 ${files.length + fileList.length} 个文件`)
    },
    handleSuccess(response, file, fileList) {
      this.$message.success('视频上传成功！')
      this.fileList = []
    },
    handleError(err, file, fileList) {
      console.log(err)
      this.$message.error('视频上传失败，请重新上传！')
      this.fileList = []
    },
    handleChange(file, fileList) {
      if (fileList.length !== 0) {
        this.btnUploadEnable = true
      } else {
        this.btnUploadEnable = false
      }
    },
    beforeUploadVideo(file) {
      const isLt10M = file.size / 1024 / 1024 < 500
      if (['video/mp4', 'video/flv', 'video/avi'].indexOf(file.type) === -1) {
        this.$message.error('请上传正确的视频格式')
        return false
      }
      if (!isLt10M) {
        this.$message.error('上传视频大小不能超过500MB!')
        return false
      }
    }
  }
}
</script>

<style>
.upload_page {
  right: 20px;
  bottom: 0;
  margin-top: 20px;
  overflow: hidden;
  background-color: white;
}

.upload {
  margin-top: 20px;
  margin-bottom: 20px;
  width: 500px;
}
</style>
