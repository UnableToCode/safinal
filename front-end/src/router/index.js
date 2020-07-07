import Vue from 'vue'
import Router from 'vue-router'
import VideoList from '@/components/VideoList'
import Upload from '@/components/Upload'
import Preview from '@/components/Preview'

Vue.use(Router)

export default new Router({
  mode: 'history',
  routes: [
    {
      path: '/list',
      name: 'List',
      component: VideoList,
      meta: {
        keepAlive: true,
        requireLogin: true
      }
    },
    {
      path: '/upload',
      name: 'Upload',
      component: Upload,
      meta: {
        keepAlive: true,
        requireLogin: true
      }
    },
    {
      path: '/preview',
      name: 'Preview',
      component: Preview,
      meta: {
        keepAlive: true,
        requireLogin: true
      }
    }
  ]
})
