<template>
  <div class="top">
    <span class="header_title">PuriPuri</span>
  </div>
</template>

<script>
export default {
  name: 'Header'
}
</script>

<style>
.top {
  background-color: #009688;
  height: 80px;
  line-height: 80px;
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
}

.username {
  color: white;
  font-size: 22px;
  float: right;
  margin-right: 40px;
}

.header_title {
  color: white;
  font-size: 36px;
  float: left;
  margin-left: 40px;
}
</style>
