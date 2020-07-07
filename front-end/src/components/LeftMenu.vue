<template>
  <el-row class="sidebar">
    <el-col :span="24">
      <el-menu :default-active="$router.path"
               router
               background-color="#F7F7F7"
               text-color="#707070"
               active-text-color="#009688">
        <el-menu-item index="/list">
          <i class="el-icon-search"></i>
          <span slot="title">视频列表</span>
        </el-menu-item>
        <el-menu-item index="/upload">
          <i class="el-icon-upload"></i>
          <span slot="title">上传视频</span>
        </el-menu-item>
      </el-menu>
    </el-col>
  </el-row>

</template>

<script>
export default {
  name: 'Sidebar',
  data() {
    return {
    }
  },

  methods: {

    handleOpen(key, keyPath) {
      console.log(key, keyPath)
    },

    handleClose(key, keyPath) {
      console.log(key, keyPath)
    }
  }
}
</script>

<style>
.sidebar {
  margin-top: 40px;
  margin-left: 30px;
}
</style>
