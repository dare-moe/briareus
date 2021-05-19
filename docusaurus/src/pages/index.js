import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './styles.module.css';

const features = [{
    title: "Simple API",
    imageUrl: "img/bria-chibi-coffee-450x600.png",
    description: "Start JVM with carefully crafted CompletableFuture based api. Focus on what JVM you want to start not how to do it."
}, {
    title: "Cute mascot",
    imageUrl: "img/bria-chibi-450x600.png",
    description: "Dare projects comes with moe in mind. Magnificent Bria-chan will bring moe to your projects."
}];

function Feature({imageUrl, title, description}) {
  const imgUrl = useBaseUrl(imageUrl);
  return (
    <div className={clsx('col col--6', styles.feature)}>
      {imgUrl && (
        <div className="text--center">
          <img className={styles.featureImage} src={imgUrl} alt={title} />
        </div>
      )}
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
}

export default function Home() {
  const context = useDocusaurusContext();
  const {siteConfig = {}} = context;
  return (
    <Layout
      title={`${siteConfig.title}`}
      description="Library for starting JVM under different resource managers">
      <header className={clsx('hero hero--primary', styles.heroBanner)}>
        <div className="container">
          <div className="row">
            <div className={clsx('col col--5 col--offset-1')}>
              <h1 className="hero__title">{siteConfig.title}</h1>
              <p className="hero__subtitle">{siteConfig.tagline}</p>
              <div className={styles.buttons}>
                <Link
                  className={clsx('button button--outline button--secondary button--lg', styles.getStarted)}
                  to={useBaseUrl('docs/')}>
                  Get Started
                </Link>
              </div>
            </div>
            <div className={clsx('col col--5')}>
              <img className={styles.heroImage} src={useBaseUrl("img/bria-400x600.png")} />
            </div>
          </div>
        </div>
      </header>
      <main>
        {features && features.length > 0 && (
          <section className={styles.features}>
            <div className="container">
              <div className="row">
                {features.map((props, idx) => (
                  <Feature key={idx} {...props} />
                ))}
              </div>
            </div>
          </section>
        )}
      </main>
    </Layout>
  );
}
