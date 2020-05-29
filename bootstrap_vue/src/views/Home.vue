<template>
<b-container>
  <b-row align-v="center">
      <job-card v-for="job in displayJobs" :key="job.id" :name="job.name" :id="job.id"></job-card>
      
    <!-- <b-col md="3">
      <div>
  <b-card
    title="Card Title"
    img-src="https://picsum.photos/600/300/?image=25"
    img-alt="Image"
    img-top
    tag="article"
    style="max-width: 20rem;"
    class="mb-2"
  >
    <b-card-text>
      Some quick example text to build on the card title and make up the bulk of the card's content.
    </b-card-text>

    <b-button href="#" variant="primary">Go somewhere</b-button>
  </b-card>
</div>
    </b-col>
    <b-col>2 of 3</b-col>
    <b-col>3 of 3</b-col>-->
</b-row>
<!-- Use text in props -->
    <b-pagination
      v-model="currentPage"
      :total-rows="rows"
      :per-page="perPage"
      first-text="First"
      prev-text="Prev"
      next-text="Next"
      last-text="Last"
      @input="paginate(currentPage)"
    ></b-pagination>
</b-container>
</template> 

<script>
// @ is an alias to /src
import JobCard from '@/components/JobCard.vue'
import {mapGetters} from "vuex"
export default {
  name: 'Home',
  components: {'job-card': JobCard },
  computed:{
    ...mapGetters(["jobs","displayJobs","rows"])
  },
  mounted(){
    this.fetchData()
  },
  data(){
    return {
      // jobs: [],
      // dispalyJobs:[],
      currentPage:1,
      // rows:1,
      perPage:3
    }
  },
  methods:{
    async fetchData(){
      this.$store.dispatch("fetchJobs")
      console.log("test",this.$store.getters.jobs)
      // const res=await fetch("jobs.json")
      // const val= await res.json()
      // this.jobs=this.jobs
      // this.dispalyJobs=this.jobs.slice(0,3)
      // this.rows=this.jobs.length
      console.log(this.jobs)
    },
    paginate(currentPage){
      // const start=(currentPage-1)*this.perPage
      // this.dispalyJobs=this.jobs.slice(start, start+3)
      this.$store.dispatch("paginate",{currentPage, perPage: this.perPage})
    }
  }
}
</script>

