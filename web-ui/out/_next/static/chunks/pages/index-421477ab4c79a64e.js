(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[405],{8456:function(e,r,t){"use strict";t.d(r,{Z:function(){return N}});var i=t(3366),a=t(7462),s=t(7294),n=t(6010),o=t(4780),l=t(917),c=t(8216),u=t(1657),d=t(948),h=t(1588),f=t(4867);function v(e){return(0,f.Z)("MuiCircularProgress",e)}(0,h.Z)("MuiCircularProgress",["root","determinate","indeterminate","colorPrimary","colorSecondary","svg","circle","circleDeterminate","circleIndeterminate","circleDisableShrink"]);var m=t(5893);let k=["className","color","disableShrink","size","style","thickness","value","variant"],x=e=>e,p,g,Z,y,w=(0,l.F4)(p||(p=x`
  0% {
    transform: rotate(0deg);
  }

  100% {
    transform: rotate(360deg);
  }
`)),P=(0,l.F4)(g||(g=x`
  0% {
    stroke-dasharray: 1px, 200px;
    stroke-dashoffset: 0;
  }

  50% {
    stroke-dasharray: 100px, 200px;
    stroke-dashoffset: -15px;
  }

  100% {
    stroke-dasharray: 100px, 200px;
    stroke-dashoffset: -125px;
  }
`)),b=e=>{let{classes:r,variant:t,color:i,disableShrink:a}=e,s={root:["root",t,`color${(0,c.Z)(i)}`],svg:["svg"],circle:["circle",`circle${(0,c.Z)(t)}`,a&&"circleDisableShrink"]};return(0,o.Z)(s,v,r)},S=(0,d.ZP)("span",{name:"MuiCircularProgress",slot:"Root",overridesResolver:(e,r)=>{let{ownerState:t}=e;return[r.root,r[t.variant],r[`color${(0,c.Z)(t.color)}`]]}})(({ownerState:e,theme:r})=>(0,a.Z)({display:"inline-block"},"determinate"===e.variant&&{transition:r.transitions.create("transform")},"inherit"!==e.color&&{color:(r.vars||r).palette[e.color].main}),({ownerState:e})=>"indeterminate"===e.variant&&(0,l.iv)(Z||(Z=x`
      animation: ${0} 1.4s linear infinite;
    `),w)),_=(0,d.ZP)("svg",{name:"MuiCircularProgress",slot:"Svg",overridesResolver:(e,r)=>r.svg})({display:"block"}),C=(0,d.ZP)("circle",{name:"MuiCircularProgress",slot:"Circle",overridesResolver:(e,r)=>{let{ownerState:t}=e;return[r.circle,r[`circle${(0,c.Z)(t.variant)}`],t.disableShrink&&r.circleDisableShrink]}})(({ownerState:e,theme:r})=>(0,a.Z)({stroke:"currentColor"},"determinate"===e.variant&&{transition:r.transitions.create("stroke-dashoffset")},"indeterminate"===e.variant&&{strokeDasharray:"80px, 200px",strokeDashoffset:0}),({ownerState:e})=>"indeterminate"===e.variant&&!e.disableShrink&&(0,l.iv)(y||(y=x`
      animation: ${0} 1.4s ease-in-out infinite;
    `),P)),D=s.forwardRef(function(e,r){let t=(0,u.Z)({props:e,name:"MuiCircularProgress"}),{className:s,color:o="primary",disableShrink:l=!1,size:c=40,style:d,thickness:h=3.6,value:f=0,variant:v="indeterminate"}=t,x=(0,i.Z)(t,k),p=(0,a.Z)({},t,{color:o,disableShrink:l,size:c,thickness:h,value:f,variant:v}),g=b(p),Z={},y={},w={};if("determinate"===v){let e=2*Math.PI*((44-h)/2);Z.strokeDasharray=e.toFixed(3),w["aria-valuenow"]=Math.round(f),Z.strokeDashoffset=`${((100-f)/100*e).toFixed(3)}px`,y.transform="rotate(-90deg)"}return(0,m.jsx)(S,(0,a.Z)({className:(0,n.Z)(g.root,s),style:(0,a.Z)({width:c,height:c},y,d),ownerState:p,ref:r,role:"progressbar"},w,x,{children:(0,m.jsx)(_,{className:g.svg,ownerState:p,viewBox:"22 22 44 44",children:(0,m.jsx)(C,{className:g.circle,style:Z,ownerState:p,cx:44,cy:44,r:(44-h)/2,fill:"none",strokeWidth:h})})}))});var N=D},8312:function(e,r,t){(window.__NEXT_P=window.__NEXT_P||[]).push(["/",function(){return t(8653)}])},8653:function(e,r,t){"use strict";t.r(r),t.d(r,{default:function(){return f}});var i=t(5893),a=t(7294),s=t(1163),n=t(5675),o=t.n(n),l=t(7357),c=t(8456),u=t(5532);let d=e=>{let{sx:r}=e;return(0,i.jsxs)(l.Z,{sx:{height:"100vh",display:"flex",alignItems:"center",flexDirection:"column",justifyContent:"center",...r},children:[(0,i.jsx)(o(),{src:u.Z,alt:"Logo",width:350,height:350}),(0,i.jsx)(c.Z,{disableShrink:!0,sx:{mt:6}})]})},h=()=>{let e=(0,s.useRouter)();return(0,a.useEffect)(()=>{e.replace("/home")},[e]),(0,i.jsx)(d,{sx:{height:"100%"}})};var f=h}},function(e){e.O(0,[774,888,179],function(){return e(e.s=8312)}),_N_E=e.O()}]);