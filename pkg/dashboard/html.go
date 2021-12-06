// html, css, and js code for dashboard

package dashboard

const (
	htmlContents = `<!DOCTYPE html>
	<html>
		<head>
			<meta name="viewport" content="width=device-width, initial-scale=1">
			<style>
				* {
				  box-sizing: border-box;
				}
	
				body {
				  margin: 0;
				  font-family: Arial;
				}
	
				/* The grid: Five equal columns that floats next to each other */
				.column {
				  float: left;
				  width: 20%;
				  padding: 10px;
				}
	
				/* Style the images inside the grid */
				.column img {
				  opacity: 0.8; 
				  cursor: pointer; 
				}
	
				.column img:hover {
				  opacity: 1;
				}
	
				/* Clear floats after the columns */
				.row:after {
				  content: "";
				  display: table;
				  clear: both;
				}
	
				/* The expanding image container */
				.container {
				  position: relative;
				  display: none;
				}
	
				/* Expanding image text */
				#imgtext {
				  position: absolute;
				  bottom: 15px;
				  left: 15px;
				  color: white;
				  font-size: 20px;
				}
	
				/* Closable button inside the expanded image */
				.closebtn {
				  position: absolute;
				  top: 10px;
				  right: 15px;
				  color: white;
				  font-size: 35px;
				  cursor: pointer;
				}

				/* Style the tab */
				.tab {
				  overflow: hidden;
				  border: 1px solid #ccc;
				  background-color: #f1f1f1;
				}

				/* Style the buttons that are used to open the tab content */
				.tab button {
				  background-color: inherit;
				  float: left;
				  border: none;
				  outline: none;
				  cursor: pointer;
				  padding: 14px 16px;
				  transition: 0.3s;
				}

				/* Change background color of buttons on hover */
				.tab button:hover {
				  background-color: #ddd;
				}

				/* Create an active/current tablink class */
				.tab button.active {
				  background-color: #ccc;
				}

				/* Style the tab content */
				.tabcontent {
				  display: none;
				  padding: 6px 12px;
				  border: 1px solid #ccc;
				  border-top: none;
				}
			</style>
		</head>
		
		<body>
			<div class="tab">
				<button class="tablinks" onclick="openCity(event, 'Requirement 3')">Requirement 3</button>
				<button class="tablinks" onclick="openCity(event, 'Manditory Requrement - ZIP Code Forcast')">Manditory Requrement - ZIP Code Forcast</button>
				<button class="tablinks" onclick="openCity(event, 'Manditory Requrement - Community Area Forcast')">Manditory Requrement - Community Area Forcast</button>
				<button class="tablinks" onclick="openCity(event, 'Manditory Requrement - CCVI Forcast')">Manditory Requrement - CCVI Forcast</button>
			</div>

			<div style="text-align:center">
				<h1>Chicago Business Intelligence Reports for Strategic Planning</h1>
				<p>Dashboard updated every 24 hours</p>
			</div>
	
			<div id="Requirement 3" class="tabcontent">
				<!-- The four columns -->
				<div class="row">
				<div class="column">
					<img src="figs/day1.png" style="width:100%" onclick="myFunction(this);">
				</div>
				<div class="column">
					<img src="figs/week1.png" style="width:100%" onclick="myFunction(this);">
				</div>
				<div class="column">
					<img src="figs/week2.png" style="width:100%" onclick="myFunction(this);">
				</div>
				<div class="column">
					<img src="figs/month1.png" style="width:100%" onclick="myFunction(this);">
				</div>
				<div class="column">
					<img src="figs/month2.png" style="width:100%" onclick="myFunction(this);">
				</div>
				</div>
			</div>

			<div id="Manditory Requrement - ZIP Code Forcast" class="tabcontent">
				<!-- The four columns -->
				<div class="row">
				<div class="column">
					<img src="figs/day1-zip.png" style="width:100%" onclick="myFunction(this);">
				</div>
				<div class="column">
					<img src="figs/week1-zip.png" style="width:100%" onclick="myFunction(this);">
				</div>
				<div class="column">
					<img src="figs/week2-zip.png" style="width:100%" onclick="myFunction(this);">
				</div>
				<div class="column">
					<img src="figs/month1-zip.png" style="width:100%" onclick="myFunction(this);">
				</div>
				<div class="column">
					<img src="figs/month2-zip.png" style="width:100%" onclick="myFunction(this);">
				</div>
				</div>
			</div>
			
			<div id="Manditory Requrement - Community Area Forcast" class="tabcontent">
				<!-- The four columns -->
				<div class="row">
				<div class="column">
					<img src="figs/day1-ca.png" style="width:100%" onclick="myFunction(this);">
				</div>
				<div class="column">
					<img src="figs/week1-ca.png" style="width:100%" onclick="myFunction(this);">
				</div>
				<div class="column">
					<img src="figs/week2-ca.png" style="width:100%" onclick="myFunction(this);">
				</div>
				<div class="column">
					<img src="figs/month1-ca.png" style="width:100%" onclick="myFunction(this);">
				</div>
				<div class="column">
					<img src="figs/month2-ca.png" style="width:100%" onclick="myFunction(this);">
				</div>
				</div>
			</div>

			<div id="Manditory Requrement - CCVI Forcast" class="tabcontent">
				<!-- The four columns -->
				<div class="row">
				<div class="column">
					<p>Forcasted CCVI for October 3</p>
					<img src="figs/day-ccvi.png" style="width:100%">
				</div>
				<div class="column">
					<p>Forcasted CCVI for Oct. 3 to Oct. 9</p>
					<img src="figs/week-ccvi.png" style="width:100%">
				</div>
				</div>
			</div>

			<div class="container">
			  <span onclick="this.parentElement.style.display='none'" class="closebtn">&times;</span>
			  <img id="expandedImg" style="width:100%">
			  <div id="imgtext"></div>
			</div>
	
			<script>
				function myFunction(imgs) {
					var expandImg = document.getElementById("expandedImg");
					var imgText = document.getElementById("imgtext");
					expandImg.src = imgs.src;
					imgText.innerHTML = imgs.alt;
					expandImg.parentElement.style.display = "block";
				}

				function openCity(evt, cityName) {
					// Declare all variables
					var i, tabcontent, tablinks;
				  
					// Get all elements with class="tabcontent" and hide them
					tabcontent = document.getElementsByClassName("tabcontent");
					for (i = 0; i < tabcontent.length; i++) {
					  tabcontent[i].style.display = "none";
					}
				  
					// Get all elements with class="tablinks" and remove the class "active"
					tablinks = document.getElementsByClassName("tablinks");
					for (i = 0; i < tablinks.length; i++) {
					  tablinks[i].className = tablinks[i].className.replace(" active", "");
					}
				  
					// Show the current tab, and add an "active" class to the button that opened the tab
					document.getElementById(cityName).style.display = "block";
					evt.currentTarget.className += " active";
				  }
			</script>
		</body>
	</html>`
)
