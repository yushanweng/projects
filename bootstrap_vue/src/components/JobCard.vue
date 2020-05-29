<template>
  <div>
    <b-col>
      <b-card
        :title="name"
        img-src="https://picsum.photos/600/300/?image=25"
        img-alt="Image"
        img-top
        tag="article"
        style="max-width: 20rem;"
        class="mb-2"
        footer="Card Footer"
      >
            <b-card-text>
                Some quick example text to build on the card title and make up the bulk of the card's content.
            </b-card-text>
            <b-button v-b-modal="modalId" variant="primary" v-b-tooltip.hover title="Apple Here">Go somewhere</b-button>
            <b-modal :id="modalId" title="Apply here info">
            <p class="my-4">Hello for modal</p>
            </b-modal>
       </b-card>
    </b-col>
   </div>
</template>

<script>
export default {
  props:["name","id"],
  computed:{
      modalId(){
          return `modal-${this.id}`
      }
  }
}
</script>

<style scoped>

</style>