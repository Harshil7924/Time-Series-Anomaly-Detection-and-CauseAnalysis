		window.onload = function () {


			var chart = new CanvasJS.Chart("chartContainer",
			{
				zoomEnabled: true,
				title:{
					text: "Anomaly Detection v1.0" 
				},
				animationEnabled: true,
				axisX:{
					title:"Time",
					labelAngle: 30
				},

				axisY :{
					title:"Target Value",
					includeZero:false
				},

				data: [{
					type: "line",
					color: "green",
					datapoints=[
					{% for el in actual %}

					{
						x: {{ el.0 }},
						y: {{ el.1 }}                
					},
					{% endfor %}
					]
				}, 
				{
					type: "scatter",color:"red",markerType:"cross",markerSize:7,
					datapoints=[
					{% for el in anomalies %}
					{
						x: {{ el.0 }},
						y: {{ el.1 }}                
					},

					{% endfor %}
					]
				}
		      ] // random generator below

		  });
			chart.render();
		}
