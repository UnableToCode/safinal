<template>
  <div class='list_page'>
    <h3 class='page_title'>检索</h3>
    <div align="left"
         style="margin-left: 20px">
      <el-table align="center"
                class="list_table"
                :data="tableDatas.slice((currentPage-1)*20,currentPage*20)">
        <el-table-column prop="videoTitle"
                         label="视频标题"
                         width="300px">
        </el-table-column>
        <el-table-column prop="uploadDate"
                         label="上传日期"
                         width="200px">
        </el-table-column>
        <el-table-column prop="view"
                         label="观看"
                         width="200px">
          <template slot-scope="scope">
            <el-button size="mini"
                       @click="view360p(scope.row)">360p</el-button>
            <el-button size="mini"
                       @click="view480p(scope.row)">720p</el-button>
          </template>
        </el-table-column>

      </el-table>
      <div align="center"
           style="margin-top:5px; margin-bottom: 5px;">
        <el-pagination @current-change="handleCurrentChange"
                       :current-page="currentPage"
                       :page-size="20"
                       layout="total, prev, pager, next, jumper"
                       :total="totalNum">
        </el-pagination>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'list_table',
  data() {
    return {
      tableDatas: [],
      currentPage: 1,
      totalNum: 0
    }
  },

  created() {
    this.getList()
  },

  methods: {
    async getList() {
      this.tableDatas = []
      try {
        const response = await fetch('http://localhost:8080/videoList', {
          method: 'GET',
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
          }
        })
        const data = await response.json()
        if (JSON.stringify(data) !== '{}') {
          this.tableDatas = data._embedded.videos
          this.totalNum = this.tableDatas.length()
          this.currentPage = 1
        }
      } catch (error) {
        console.log(error)
      }
    },
    view360p(row) {
      this.$router.push({
        name: 'Preview',
        params: {
          id: row.id,
          height: 360
        }
      })
    },
    view480p(row) {
      this.$router.push({
        name: 'Preview',
        params: {
          id: row.id,
          height: 480
        }
      })
    },
    handleCurrentChange(val) {
      this.currentPage = val
    }
  }
}

</script>

<style>
.list_page {
  right: 20px;
  bottom: 0;
  margin-top: 20px;
  overflow: hidden;
  background-color: white;
}

.page_title {
  margin-top: 10px;
  margin-left: 20px;
  font-size: 20px;
  text-align: left;
}

.list_table {
  margin-top: 10px;
}
</style>
