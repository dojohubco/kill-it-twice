import { bootstrapApplication } from '@angular/platform-browser';
import { provideRouter, withInMemoryScrolling } from '@angular/router';
import { AppComponent } from './app';
import { routes } from './routes';
void bootstrapApplication(AppComponent, {
  providers: [
    provideRouter(
      routes,
      withInMemoryScrolling({ scrollPositionRestoration: 'enabled' }),
    ),
  ],
}).catch(() => {
  const root = document.querySelector('kit-root');
  if (root)
    root.textContent =
      'Unable to start the operator interface. Reload this page or check the application build.';
});
