// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'
import ElementUI from 'element-ui'
import 'element-ui/lib/theme-chalk/index.css'

import App from './App'
import router from './router'
import './plugins/element.js'
import VideoPlayer from 'vue-video-player'

require('video.js/dist/video-js.css')
require('vue-video-player/src/custom-theme.css')

Vue.config.productionTip = false

Vue.use(VideoPlayer)
Vue.use(ElementUI)

router.beforeEach((to, from, next) => {
  if (to.matched.length === 0) {
    from.name ? next({ name: from.name }) : next('/list')
  } else {
    next()
  }
})

/* eslint-disable no-new */
new Vue({
  el: '#app',
  router,
  components: {
    App
  },
  template: '<App/>'
})
