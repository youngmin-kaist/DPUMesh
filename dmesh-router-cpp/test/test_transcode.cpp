// Offline round trip: nghttp2 deflater (client) -> transcode -> nghttp2 inflater (backend).
#include <nghttp2/nghttp2.h>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include "relay3.hpp"
namespace dmesh { bool transcode_impl(const uint8_t *b, int len, HpTable &mirror, HpTable *enc, std::vector<uint8_t> &out, PolicyFields *pf, TranscodeStats &ts); void log_warn(const char*, ...) {} void log_info(const char*, ...) {} }
using namespace dmesh;
#define NV(n,v) {(uint8_t*)n,(uint8_t*)v,sizeof(n)-1,sizeof(v)-1,NGHTTP2_NV_FLAG_NONE}
static std::vector<std::pair<std::string,std::string>> inflate_all(nghttp2_hd_inflater *inf, const uint8_t *p, size_t n, bool *ok){
    std::vector<std::pair<std::string,std::string>> out; *ok=true;
    for(;;){ nghttp2_nv nv; int fl=0; ssize_t r=nghttp2_hd_inflate_hd2(inf,&nv,&fl,p,n,1); if(r<0){*ok=false; std::printf("  inflate error %zd (%s)\n",r,nghttp2_strerror((int)r)); return out;} p+=r; n-=r; if(fl&NGHTTP2_HD_INFLATE_EMIT) out.emplace_back(std::string((char*)nv.name,nv.namelen),std::string((char*)nv.value,nv.valuelen)); if(fl&NGHTTP2_HD_INFLATE_FINAL){nghttp2_hd_inflate_end_headers(inf);break;} if(n==0&&!(fl&NGHTTP2_HD_INFLATE_FINAL)){ /* need final */ } }
    return out;
}
int main(){
    nghttp2_hd_deflater *cdef; nghttp2_hd_inflater *binf; nghttp2_hd_deflate_new(&cdef,4096); nghttp2_hd_inflate_new(&binf);
    HpTable mirror(2), enc(3); enc.set_max(4096);
    TranscodeStats ts; PolicyFields pf;
    char tid[64]; int fails=0;
    for(int r=0;r<300;r++){
        std::snprintf(tid,sizeof tid,"%016x:%016x:0:1",r*2654435761u,r*40503u);
        std::string path = r%3==0 ? "/search.Search/Nearby" : r%3==1 ? "/ok" : "/api/v1/items?id=12345";
        nghttp2_nv nva[]={NV(":method","POST"),NV(":scheme","http"),{(uint8_t*)":path",(uint8_t*)path.data(),5,path.size(),NGHTTP2_NV_FLAG_NONE},NV(":authority","srv-search:8082"),NV("content-type","application/grpc"),NV("user-agent","grpc-go/1.71.0"),NV("te","trailers"),{(uint8_t*)"uber-trace-id",(uint8_t*)tid,13,std::strlen(tid),NGHTTP2_NV_FLAG_NONE},NV("x-secret","hunter2")};
        nva[8].flags=NGHTTP2_NV_FLAG_NO_INDEX; // never-indexed
        uint8_t blk[1024]; ssize_t bl=nghttp2_hd_deflate_hd(cdef,blk,sizeof blk,nva,9);
        if(r==5){ // client shrinks its table to 0 for this block: inserts get evicted immediately (curl does this)
            nghttp2_hd_deflate_change_table_size(cdef,0); bl=nghttp2_hd_deflate_hd(cdef,blk,sizeof blk,nva,9); nghttp2_hd_inflate_change_table_size(binf,4096); }
        if(r==6){ nghttp2_hd_deflate_change_table_size(cdef,4096); bl=nghttp2_hd_deflate_hd(cdef,blk,sizeof blk,nva,9); }
        std::vector<uint8_t> out; 
        if(!transcode_impl(blk,(int)bl,mirror,&enc,out,&pf,ts)){ std::printf("r=%d transcode FAILED\n",r); fails++; break; }
        bool ok; auto got=inflate_all(binf,out.data(),out.size(),&ok);
        if(!ok){ std::printf("r=%d inflate failed (in %zd B, out %zu B)\n",r,bl,out.size()); fails++; break; }
        if(got.size()!=9){ std::printf("r=%d field count %zu\n",r,got.size()); fails++; break; }
        for(int k=0;k<9;k++){ if(got[k].first!=std::string((char*)nva[k].name,nva[k].namelen)||got[k].second!=std::string((char*)nva[k].value,nva[k].valuelen)){ std::printf("r=%d field %d mismatch: %s=%s\n",r,k,got[k].first.c_str(),got[k].second.c_str()); fails++; } }
        if(pf.path!=path||pf.method!="POST"||pf.authority!="srv-search:8082"){ std::printf("r=%d policy fields wrong: %s %s %s\n",r,pf.method.c_str(),pf.path.c_str(),pf.authority.c_str()); fails++; }
        if(fails) break;
        if(r<2||r==299) std::printf("r=%d in=%zd out=%zu B ok\n",r,bl,out.size());
    }
    std::printf("fields=%llu static=%llu dyn_hit=%llu dyn_miss=%llu lit_insert=%llu lit_noidx=%llu never=%llu in=%llu out=%llu fails=%d\n",(unsigned long long)ts.fields,(unsigned long long)ts.static_idx,(unsigned long long)ts.dyn_hit,(unsigned long long)ts.dyn_miss,(unsigned long long)ts.lit_insert,(unsigned long long)ts.lit_noindex,(unsigned long long)ts.never_indexed,(unsigned long long)ts.in_bytes,(unsigned long long)ts.out_bytes,fails);
    return fails?1:0;
}
