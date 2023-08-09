import { createMuiTheme } from "@material-ui/core/styles";
import { CalciteTheme } from "calcite-react/CalciteThemeProvider";
import { unitCalc } from "calcite-react/utils/helpers";

// A custom theme for this app
const CustomTheme = createMuiTheme({
  palette: {
    green: {
      main: "#007150",
      medium: "#007150",
    },
    blue: {
      main: "#00264C",
      lightest: "#2D90EC",
      light: "#1D5AAB",
      medium: "#005EA2",
      dark: "#00264C",
    },
    grey: {
      main: "#565C65",
      dark: "#565C65",
      medium: "#71767A",
      mediumLight: "#A9AEB1",
      light: "#D6D7D9",
    },
    black: {
      main: "#1B1B1B",
      primary: "#1B1B1B",
      secondary: "#303030",
    },
    error: {
      main: "#D83933",
    },
    background: {
      default: "#FFFFFF",
      secondary: "#EEEEEE",
    },
    primary: { main: "#005EA2" },
    secondary: { main: "#565C65" },

    text: {
      default: "#1B1B1B",
      primary: "#1D5AAB",
      secondary: "#565C65",
      elements: "#005EA2",
    },
  },
  typography: {
    fontFamily: ["public-sans", "sans-serif"].join(","),
    h1: {
      fontSize: "22px",
    },
    text: {
      default: "#1B1B1B",
      primary: "#1D5AAB",
      secondary: "#565C65",
      elements: "#005EA2",
    },
  },
  panelMargin: unitCalc(CalciteTheme.baseline, 2, "/"),
  panelPadding: unitCalc(CalciteTheme.baseline, 2, "/"),
  boxShadow: "0 1px 2px rgba(0, 0, 0, 0.3)",
  border: "1px solid #eaeaea",
  overrides: {
    MuiInputBase: {
      root: {
        borderRadius: 0,
      },
    },
    MuiButtonBase: {},
  },
  props: {
    MuiButtonBase: {
      disableRipple: true, // No more ripple
    },
  },
});

export default CustomTheme;

