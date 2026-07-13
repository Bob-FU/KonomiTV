<template>
    <div class="adhoc-player-loading d-flex justify-center align-center w-100">
        <v-progress-circular indeterminate color="primary" size="48" />
    </div>
</template>
<script lang="ts">

import { defineComponent } from 'vue';

import Videos from '@/services/Videos';

export default defineComponent({
    name: 'Videos-PlayFile',
    async created() {

        const path = this.$route.query.path;
        const hash = this.$route.query.hash;
        if (typeof path !== 'string' || typeof hash !== 'string' || path === '' || hash === '') {
            await this.$router.replace('/not-found/');
            return;
        }

        const video_id = await Videos.registerAdhocFile(path, hash);
        if (video_id === null) {
            await this.$router.replace('/not-found/');
            return;
        }

        await this.$router.replace('/videos/watch/' + video_id);
    },
});

</script>
<style lang="scss" scoped>

.adhoc-player-loading {
    min-height: 100vh;
}

</style>
