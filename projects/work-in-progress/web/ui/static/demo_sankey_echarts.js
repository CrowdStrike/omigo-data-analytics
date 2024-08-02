// echarts need chart-container as id 
var dom = document.getElementById("chart-container");
var myChart = echarts.init(dom, null, {
  renderer: "canvas",
  useDirtyRect: false
});
var app = {};

var option;

myChart.showLoading();
const queryString = window.location.search;
const urlParams = new URLSearchParams(queryString);
const myurl = "http://127.0.0.1:9001/get_avengers?output_format=graph";
window.onload = function() {
  d3.json(myurl, {
    method: "GET",
    credentials: "include",
    headers: { "Accept": "application/json" }
  }).then(function(graph) {
    myChart.hideLoading();
    console.log(graph)
    myChart.setOption(
    (option = {
      series: {
        type: "sankey",
        layout: "none",
        emphasis: {
          focus: "adjacency"
        },
        data: graph.nodes,
        links: graph.links
      }
   }));
 });

 if (option && typeof option === "object") {
   myChart.setOption(option);
 }
}
window.addEventListener("resize", myChart.resize);
