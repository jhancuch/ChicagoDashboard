// Script runs the two required python scripts that geocode and conduct the analysis through PowerShell

package analysis

import (
	"fmt"
	"log"
	"os/exec"
)

func CallPy() {

	// set paths and commends
	powershell := `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`
	python := "python"

	geocodeCA := `C:\Users\jwnha\Documents\GitHub\chicago-dashboard\pkg\analysis\geocodeCA.py`
	geocodeZIP := `C:\Users\jwnha\Documents\GitHub\chicago-dashboard\pkg\analysis\geocodeZIP.py`
	requirement3 := `C:\Users\jwnha\Documents\GitHub\chicago-dashboard\pkg\analysis\requirement3.py`
	mandatoryRequirementTripsCA := `C:\Users\jwnha\Documents\GitHub\chicago-dashboard\pkg\analysis\mandatoryRequirementTripsCA.py`
	mandatoryRequirementTripsZIP := `C:\Users\jwnha\Documents\GitHub\chicago-dashboard\pkg\analysis\mandatoryRequirementTripsZIP.py`
	mandatoryRequirementCCVI := `C:\Users\jwnha\Documents\GitHub\chicago-dashboard\pkg\analysis\mandatoryRequirementCCVI.py`

	// call powershell to run python script geocodeCA and geocodeZIP
	cmd1 := exec.Command(powershell, python, geocodeCA)
	_, err1 := cmd1.Output()
	if err1 != nil {
		fmt.Println(err1)
		log.Fatal(err1)
	}

	cmd2 := exec.Command(powershell, python, geocodeZIP)
	_, err2 := cmd2.Output()
	if err2 != nil {
		fmt.Println(err2)
		log.Fatal(err2)
	}

	// call powershell to run python scripts that create the models for each zip code and community area and the generate figs.
	cmd3 := exec.Command(powershell, python, requirement3)
	_, err3 := cmd3.Output()
	if err3 != nil {
		fmt.Println(err3)
		log.Fatal(err3)
	}

	cmd4 := exec.Command(powershell, python, mandatoryRequirementTripsCA)
	_, err4 := cmd4.Output()
	if err4 != nil {
		fmt.Println(err4)
		log.Fatal(err4)
	}

	cmd5 := exec.Command(powershell, python, mandatoryRequirementTripsZIP)
	_, err5 := cmd5.Output()
	if err5 != nil {
		fmt.Println(err5)
		log.Fatal(err5)
	}

	cmd6 := exec.Command(powershell, python, mandatoryRequirementCCVI)
	_, err6 := cmd6.Output()
	if err6 != nil {
		fmt.Println(err6)
		log.Fatal(err6)
	}
}
