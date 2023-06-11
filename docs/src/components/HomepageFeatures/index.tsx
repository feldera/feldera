import React from "react";
import clsx from "clsx";
import styles from "./styles.module.css";
import Link from "@docusaurus/Link";

type FeatureItem = {
  title: string;
  Svg: React.ComponentType<React.ComponentProps<"svg">>;
  description: JSX.Element;
};

const FeatureList: FeatureItem[] = [
  {
    title: "Deploy",
    Svg: require("@site/static/img/deploy.svg").default,
    description: (
      <>
        Setup and and Run DBSP in just a few minutes in a Docker container.
      </>
    ),
  },
  {
    title: "Develop",
    Svg: require("@site/static/img/sql.svg").default,
    description: (
      <>
        Transforms, aggregate, filters and join all your data in-motion using SQL tables and views.
      </>
    ),
  },
  {
    title: "Explore",
    Svg: require("@site/static/img/study.svg").default,
    description: (
      <>
        Study our Demos to see the full potential of DBSP.
      </>
    ),
  },
];

function Feature({ title, Svg, description }: FeatureItem) {
  return (
    <div className={clsx("col col--4")}>
      <div className={styles.box}>
        <div className="text--center">
          <Svg className={styles.featureSvg} role="img" />
        </div>
        <div className="text--center padding-horiz--md">
          <h3>{title}</h3>
          <p>{description}</p>
        </div>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className={clsx("row")}>
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}