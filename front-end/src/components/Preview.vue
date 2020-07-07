<template>
  <div class="preview">
    <div style="margin-left:20px; margin-top:20px; margin-bottom: 15px; text-align: left">
      <video-player class="video-player vjs-custom-skin"
                    ref="videoPlayer"
                    :playsinline="true"
                    :options="playerOptions"></video-player>
    </div>
  </div>
</template>

<script>
export default {
  data() {
    return {
      playerOptions: {
        playbackRates: [0.7, 1.0, 1.5, 2.0],
        autoplay: false,
        muted: false,
        loop: false,
        preload: 'auto',
        language: 'zh-CN',
        aspectRatio: '16:9',
        fluid: true,
        sources: [
          {
            type: 'video/mp4',
            src: ''
          }
        ],
        notSupportedMessage: '此视频暂无法播放，请稍后再试',
        controlBar: {
          timeDivider: true,
          durationDisplay: true,
          remainingTimeDisplay: false,
          fullscreenToggle: true
        }
      }
    }
  },

  created() {
    this.playerOptions.sources[0].src = 'http://localhost:8080/preview/' + this.$route.params.height + '/' + this.$route.params.id
  },

  methods: {

  }
}
</script>

<style>
.preview {
  right: 20px;
  bottom: 0;
  margin-top: 20px;
  overflow: hidden;
  background-color: white;
}
</style>
