<template>
  <div id="app">

    <!-- 其他页 -->
    <el-container v-if="$route.meta.keepAlive">
      <el-header>
        <keep-alive>
          <!-- 导航栏 -->
          <v_Header></v_Header>
        </keep-alive>
      </el-header>
      <el-container>
        <el-aside width="200px">
          <!-- 侧边栏 -->
          <keep-alive>
            <v_leftmenu></v_leftmenu>
          </keep-alive>
        </el-aside>
        <el-main>
          <!-- Body -->
          <router-view></router-view>
        </el-main>
      </el-container>
    </el-container>

    <!-- 登录页 -->
    <router-view v-if="!$route.meta.keepAlive"></router-view>

  </div>
</template>

<script>
import LeftMenu from './components/LeftMenu'
import Header from './components/Header'

export default {
  name: 'App',
  components: {
    'v_Header': Header,
    'v_leftmenu': LeftMenu
  }
}
</script>

<style>
body {
  background: #dfdfdf;
}

#app {
  font-family: "Avenir", Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
}
</style>
